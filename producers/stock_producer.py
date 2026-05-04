"""
Finnhub → Kafka producer.

Polls a configured list of tickers every N seconds, fetches the latest
quote from Finnhub's REST API, and produces JSON messages to a Kafka
topic on Confluent Cloud.

Run:
    python stock_producer.py

Why Finnhub instead of yfinance:
    yfinance scrapes Yahoo, which actively blocks bots and changes endpoints.
    Finnhub is an official REST API with a documented contract, predictable
    rate limits (60 calls/min on free tier), and stable response schemas.
"""
from __future__ import annotations

import json
import logging
import os
import random
import signal
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import finnhub
import yaml
from confluent_kafka import Producer
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

CONFIG_PATH = Path(__file__).resolve().parent / "config" / "tickers.yaml"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("stock_producer")

# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------

@dataclass
class StockTick:
    """One observation of a ticker at a point in time."""
    ticker: str
    price: float          # current price ('c' from Finnhub)
    high: float           # day's high
    low: float            # day's low
    open_price: float     # day's open
    prev_close: float     # previous close
    timestamp: str        # ISO 8601 UTC capture time
    quote_timestamp: int  # epoch seconds from Finnhub
    source: str = "finnhub"

# ---------------------------------------------------------------------------
# Finnhub fetcher
# ---------------------------------------------------------------------------

# One client reused across all polls. Finnhub's client is thread-safe.
FINNHUB = finnhub.Client(api_key=os.environ["FINNHUB_API_KEY"])


def fetch_tick(ticker: str, retries: int = 2) -> Optional[StockTick]:
    """
    Fetch the latest quote for a single ticker from Finnhub.

    Finnhub's /quote endpoint returns:
        c: current price
        h: high price of the day
        l: low price of the day
        o: open price of the day
        pc: previous close price
        t: timestamp (epoch seconds)

    On weekends/holidays, c == pc (last close); we still emit so the
    pipeline shows continuous activity.
    """
    last_error = None
    for attempt in range(retries + 1):
        try:
            q = FINNHUB.quote(ticker)

            # Sanity check — invalid tickers return all zeros
            if q.get("c") in (None, 0):
                log.warning("ticker=%s got zero price, skipping", ticker)
                return None

            return StockTick(
                ticker=ticker,
                price=float(q["c"]),
                high=float(q.get("h", 0)),
                low=float(q.get("l", 0)),
                open_price=float(q.get("o", 0)),
                prev_close=float(q.get("pc", 0)),
                timestamp=datetime.now(timezone.utc).isoformat(),
                quote_timestamp=int(q.get("t", 0)),
            )
        except Exception as e:
            last_error = e
            if attempt < retries:
                backoff = (2 ** attempt) + random.uniform(0, 1)
                log.debug("ticker=%s attempt %d failed (%s), retrying in %.1fs",
                          ticker, attempt + 1, e, backoff)
                time.sleep(backoff)
            continue

    log.warning("ticker=%s fetch failed after %d attempts: %s",
                ticker, retries + 1, last_error)
    return None

# ---------------------------------------------------------------------------
# Kafka producer (unchanged from before)
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
        "client.id": "pulsetrade-stock-producer",
    })


def delivery_callback(err, msg) -> None:
    if err is not None:
        log.error("delivery failed: topic=%s key=%s error=%s",
                  msg.topic(), msg.key(), err)


def produce_tick(producer: Producer, topic: str, tick: StockTick) -> None:
    payload = json.dumps(asdict(tick)).encode("utf-8")
    producer.produce(
        topic=topic,
        key=tick.ticker.encode("utf-8"),
        value=payload,
        on_delivery=delivery_callback,
    )
    producer.poll(0)


# ---------------------------------------------------------------------------
# Graceful shutdown (unchanged)
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

    tickers: list[str] = config["tickers"]
    interval: int = config["poll_interval_seconds"]
    topic: str = config["kafka_topic"]

    log.info("starting stock producer | tickers=%s | interval=%ds | topic=%s",
             tickers, interval, topic)

    producer = build_kafka_producer()
    shutdown = GracefulShutdown()

    cycle = 0
    sent_total = 0
    skipped_total = 0

    try:
        while not shutdown.shutdown:
            cycle += 1
            cycle_start = time.monotonic()
            sent_this_cycle = 0
            skipped_this_cycle = 0

            for ticker in tickers:
                if shutdown.shutdown:
                    break
                tick = fetch_tick(ticker)
                if tick is None:
                    skipped_this_cycle += 1
                    continue
                produce_tick(producer, topic, tick)
                sent_this_cycle += 1
                log.info("✓ %s @ $%.2f (h=%.2f l=%.2f pc=%.2f)",
                         tick.ticker, tick.price, tick.high, tick.low, tick.prev_close)

            sent_total += sent_this_cycle
            skipped_total += skipped_this_cycle

            log.info("cycle=%d sent=%d skipped=%d total_sent=%d total_skipped=%d",
                     cycle, sent_this_cycle, skipped_this_cycle,
                     sent_total, skipped_total)

            elapsed = time.monotonic() - cycle_start
            sleep_for = max(0, interval - elapsed)
            for _ in range(int(sleep_for)):
                if shutdown.shutdown:
                    break
                time.sleep(1)

    finally:
        log.info("flushing producer (5s timeout)...")
        remaining = producer.flush(timeout=5)
        if remaining > 0:
            log.warning("%d messages still in queue at shutdown", remaining)
        log.info("shutdown complete | sent=%d skipped=%d cycles=%d",
                 sent_total, skipped_total, cycle)
    return 0


if __name__ == "__main__":
    sys.exit(main())
