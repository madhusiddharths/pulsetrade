"""
NewsAPI → Kafka producer.

Polls NewsAPI for business and technology headlines, tags each article
with the tickers it mentions (simple keyword matching), and produces JSON
messages to the `market-news` Kafka topic.

Run:
    python news_producer.py

Rate limit:
    NewsAPI free tier = 100 requests/day. We poll every 15 min × 2 categories
    = 192 requests/day. To stay under 100, we serialize categories into a
    single 15-min cycle that splits the budget: each category polled once
    per cycle = 96 requests/day total. Safe headroom for retries.

Deduplication:
    Articles can appear in successive polls. We track recently-seen URLs
    in an in-memory set bounded to the last N entries. URLs are stable
    enough to deduplicate on directly without hashing.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
import yaml
from confluent_kafka import Producer
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

CONFIG_PATH = Path(__file__).resolve().parent / "config" / "tickers.yaml"

NEWSAPI_BASE = "https://newsapi.org/v2/top-headlines"

# Cap of URLs we remember to deduplicate. ~5x the typical headline count
# per cycle is plenty without ballooning memory.
DEDUP_MAX_SIZE = 500

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("news_producer")

# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------

@dataclass
class NewsArticle:
    """One article tagged with the tickers it mentions."""
    article_id: str           # NewsAPI URL — stable unique identifier
    title: str
    description: Optional[str]
    source_name: Optional[str]
    url: str
    published_at: str         # ISO 8601 from NewsAPI
    ingested_at: str          # ISO 8601 UTC capture time
    category: str             # business / technology
    tickers: list[str] = field(default_factory=list)   # all matched tickers
    primary_ticker: Optional[str] = None               # most specific match
    source: str = "newsapi"

# ---------------------------------------------------------------------------
# Ticker matching
# ---------------------------------------------------------------------------

def match_tickers(text: str, keyword_map: dict[str, list[str]]) -> tuple[list[str], Optional[str]]:
    """
    Match a text body (title + description) against ticker keywords.

    Returns:
        (all_matched_tickers, primary_ticker)

    The primary ticker is the one whose first-listed keyword matched —
    keywords are ordered most-specific-first in the YAML config. If no
    ticker matched, returns ([], None).

    Match is case-insensitive and uses word-boundary-ish substring search.
    Not perfect (will match "Apple Records" as AAPL) but good enough for
    a research-grade pipeline.
    """
    if not text:
        return [], None

    lower_text = text.lower()
    matched: list[str] = []
    primary: Optional[str] = None
    primary_priority = float("inf")

    for ticker, keywords in keyword_map.items():
        for idx, kw in enumerate(keywords):
            if kw.lower() in lower_text:
                if ticker not in matched:
                    matched.append(ticker)
                # Track primary ticker by which keyword (lower idx = more specific)
                # matched first across all tickers
                if idx < primary_priority:
                    primary = ticker
                    primary_priority = idx
                break

    return matched, primary

# ---------------------------------------------------------------------------
# NewsAPI fetcher
# ---------------------------------------------------------------------------

def fetch_category(api_key: str, category: str, country: str = "us") -> list[dict]:
    """
    Fetch top headlines for a single category.
    Returns the raw `articles` list from NewsAPI, or [] on failure.
    """
    try:
        r = requests.get(
            NEWSAPI_BASE,
            params={"country": country, "category": category, "apiKey": api_key, "pageSize": 50},
            timeout=10,
        )
        r.raise_for_status()
        body = r.json()
        if body.get("status") != "ok":
            log.warning("NewsAPI status=%s message=%s",
                        body.get("status"), body.get("message"))
            return []
        return body.get("articles", [])
    except requests.RequestException as e:
        log.warning("NewsAPI fetch failed for category=%s: %s", category, e)
        return []

# ---------------------------------------------------------------------------
# Kafka producer (same pattern as stock_producer)
# ---------------------------------------------------------------------------

def build_kafka_producer() -> Producer:
    required = ["KAFKA_BOOTSTRAP_SERVERS", "KAFKA_API_KEY", "KAFKA_API_SECRET"]
    for var in required:
        if not os.environ.get(var):
            raise RuntimeError(f"missing env var: {var}")

    return Producer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["KAFKA_API_KEY"],
        "sasl.password": os.environ["KAFKA_API_SECRET"],
        "acks": "all",
        "retries": 5,
        "linger.ms": 50,
        "compression.type": "lz4",
        "client.id": "pulsetrade-news-producer",
    })


def delivery_callback(err, msg) -> None:
    if err is not None:
        log.error("delivery failed: topic=%s error=%s", msg.topic(), err)


def produce_article(producer: Producer, topic: str, article: NewsArticle) -> None:
    payload = json.dumps(asdict(article)).encode("utf-8")
    # Key by primary_ticker if matched; otherwise use a special key so unkeyed
    # articles don't all funnel into one partition (we have 1 partition per
    # topic right now, but this future-proofs partitioning later).
    key = (article.primary_ticker or "GENERAL").encode("utf-8")
    producer.produce(
        topic=topic,
        key=key,
        value=payload,
        on_delivery=delivery_callback,
    )
    producer.poll(0)

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

class GracefulShutdown:
    def __init__(self) -> None:
        self.shutdown = False
        signal.signal(signal.SIGINT, self._handler)
        signal.signal(signal.SIGTERM, self._handler)

    def _handler(self, signum, frame) -> None:
        log.info("received signal %d, shutting down...", signum)
        self.shutdown = True

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> int:
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    keyword_map: dict[str, list[str]] = config["ticker_keywords"]
    interval: int = config["news_poll_interval_seconds"]
    topic: str = config["news_kafka_topic"]
    categories: list[str] = config["news_categories"]

    api_key = os.environ.get("NEWSAPI_KEY")
    if not api_key:
        raise RuntimeError("missing env var: NEWSAPI_KEY")

    log.info("starting news producer | categories=%s | interval=%ds | topic=%s",
             categories, interval, topic)

    producer = build_kafka_producer()
    shutdown = GracefulShutdown()

    # Bounded dedup buffer — drops oldest URL when full
    seen_urls: deque[str] = deque(maxlen=DEDUP_MAX_SIZE)
    seen_set: set[str] = set()

    cycle = 0
    sent_total = 0
    skipped_dedup_total = 0
    skipped_no_ticker_total = 0

    try:
        while not shutdown.shutdown:
            cycle += 1
            cycle_start = time.monotonic()
            sent_this_cycle = 0
            skipped_dedup = 0
            skipped_no_ticker = 0

            for category in categories:
                if shutdown.shutdown:
                    break

                articles = fetch_category(api_key, category)
                log.info("fetched category=%s articles=%d", category, len(articles))

                for art in articles:
                    if shutdown.shutdown:
                        break

                    url = art.get("url")
                    if not url:
                        continue

                    # Dedup
                    if url in seen_set:
                        skipped_dedup += 1
                        continue
                    if len(seen_urls) == seen_urls.maxlen:
                        seen_set.discard(seen_urls[0])
                    seen_urls.append(url)
                    seen_set.add(url)

                    # Match tickers
                    title = art.get("title") or ""
                    description = art.get("description") or ""
                    haystack = f"{title} {description}"
                    tickers, primary = match_tickers(haystack, keyword_map)

                    if not tickers:
                        skipped_no_ticker += 1
                        log.debug("no ticker match: %s", title[:80])
                        continue

                    article = NewsArticle(
                        article_id=url,
                        title=title,
                        description=description or None,
                        source_name=(art.get("source") or {}).get("name"),
                        url=url,
                        published_at=art.get("publishedAt", ""),
                        ingested_at=datetime.now(timezone.utc).isoformat(),
                        category=category,
                        tickers=tickers,
                        primary_ticker=primary,
                    )
                    produce_article(producer, topic, article)
                    sent_this_cycle += 1
                    log.info("✓ [%s] %s — %s", primary, ",".join(tickers), title[:80])

            sent_total += sent_this_cycle
            skipped_dedup_total += skipped_dedup
            skipped_no_ticker_total += skipped_no_ticker

            log.info(
                "cycle=%d sent=%d skipped_dedup=%d skipped_no_ticker=%d | "
                "totals sent=%d dedup=%d no_ticker=%d",
                cycle, sent_this_cycle, skipped_dedup, skipped_no_ticker,
                sent_total, skipped_dedup_total, skipped_no_ticker_total,
            )

            elapsed = time.monotonic() - cycle_start
            sleep_for = max(0, interval - elapsed)
            log.info("sleeping for %d seconds...", int(sleep_for))
            for _ in range(int(sleep_for)):
                if shutdown.shutdown:
                    break
                time.sleep(1)

    finally:
        log.info("flushing producer (5s timeout)...")
        remaining = producer.flush(timeout=5)
        if remaining > 0:
            log.warning("%d messages still in queue at shutdown", remaining)
        log.info("shutdown complete | cycles=%d sent=%d", cycle, sent_total)

    return 0


if __name__ == "__main__":
    sys.exit(main())
