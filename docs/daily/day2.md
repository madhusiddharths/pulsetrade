# Day 2 — Kafka producers + Databricks bronze (✅ complete)

## What I built

Today the project actually started moving data. By end of day, two Python
producers are pushing live market data into Confluent Kafka, and a Spark
notebook on Databricks reads from both topics and writes to bronze Delta
tables.

### Block 1 — Stock producer (~90 min)

`producers/stock_producer.py`. Polls Finnhub every 30s for 5 tickers
(AAPL, GOOGL, MSFT, NVDA, TSLA), publishes to the `stock-prices` Kafka
topic with `ticker` as the partition key (so all messages for one ticker
land on the same partition for ordering).

Key patterns:
- **YAML-driven config** at `producers/config/stocks.yaml` for the ticker
  list + polling interval. Means changing tickers doesn't require editing
  Python.
- **`confluent-kafka` producer with `acks=all` + retries**. At-least-once
  delivery semantics. Picked over `kafka-python` because confluent-kafka
  wraps the librdkafka C client — significantly faster, and it's what
  Confluent Cloud's docs recommend.
- **Delivery callback** logs success/failure of each message. Critical for
  debugging — without it, a silent send failure looks identical to a
  successful one.
- **Graceful shutdown** on SIGINT: producer flushes pending messages
  before exiting. Otherwise `Ctrl+C` mid-flush loses messages.
- **Cycle counter in logs** ("cycle 47: 5 messages sent") so you can see
  at a glance how many polling rounds have happened.

#### yfinance → Finnhub switch (ADR-001)

Started with yfinance because it's the obvious choice in tutorials. After
~30 minutes of debugging weird `KeyError` and 429 rate-limit errors, swapped
to Finnhub. yfinance scrapes Yahoo's web UI, which is unstable; Finnhub is
a real API with documented rate limits (60 calls/min free tier — far more
than we need at 5 calls per 30s).

Documented as `docs/decisions.md` ADR-001 because this is the kind of
"why not the obvious choice" thing recruiters ask about.

### Block 2 — News producer (~60 min)

`producers/news_producer.py`. Same structural pattern as the stock producer
but for news. Polls NewsAPI every 30 min for business + tech headlines,
filters articles to ones mentioning our 5 tickers, publishes to
`market-news`.

Key patterns added:
- **Ticker keyword matching**: `"Apple"` → `AAPL`, `"NVIDIA"` → `NVDA`, etc.
  Each article gets a list of relevant tickers attached. Imperfect (misses
  things like "iPhone maker" implying AAPL) but good enough for a portfolio
  project.
- **Article deduplication**: track seen `article_id`s in a set; skip
  re-publishing. NewsAPI returns the same article in subsequent polls if
  it's within the time window. Without dedup, you'd republish each article
  ~96 times (every 15 min for 24h).
- **NewsAPI 100 req/day budget**: poll every 30 min = 48 req/day. Half the
  budget left as headroom in case I want to add more queries later.
- **~87% filter rate**: most business/tech news isn't about my 5 tickers.
  Logs the rejection rate so I can tune ticker keyword lists later.

Both producers verified in Confluent UI — clicked into each topic's Messages
tab and saw real JSON flowing.

Screenshots saved to `docs/screenshots/01_stock_producer_kafka.png` and
`02_news_producer_kafka.png`.

### Block 3 — Databricks Spark Structured Streaming → Bronze (~90 min)

This is where it got interesting. Wrote `01_kafka_to_bronze` notebook that
reads both Kafka topics with Spark Structured Streaming and writes to two
Delta tables: `bronze_stock_prices` and `bronze_market_news`.

Key patterns:
- **Spark Structured Streaming + `trigger(availableNow=True)`**: not
  continuous streaming — micro-batch that reads everything new since last
  run, processes, then stops. Standard production pattern (continuous
  streaming costs more compute than most use cases need).
- **Kafka source with SASL_SSL auth**: same TLS+SASL credentials as the
  producers, just spelled differently for Spark's Kafka config map. Took
  ~15 min of trial-and-error on the option names.
- **JSON schemas defined explicitly**, not inferred. `from_json(value,
  schema)` with an explicit StructType for each topic. Schema inference
  on streaming sources is unreliable.
- **Checkpoint location in `/Volumes/workspace/pulsetrade/checkpoints/`**.
  Not `/tmp/` and not DBFS — both fail on Free Edition because the home
  dir is read-only and DBFS is disabled. Unity Catalog Volumes are the
  only persistent writable storage on Free Edition.
- **Append-only writes** (`mode("append")`). Bronze is the immutable audit
  log; deduplication happens at the silver layer.

#### Pretty much everything I learned about Databricks Free Edition restrictions

Free Edition uses Spark Connect, which is more restrictive than full Spark:

- ❌ No `.rdd` API → use DataFrame methods (`batch_df.isEmpty()` not
  `batch_df.rdd.isEmpty()`)
- ❌ No `.persist()` / `.cache()` on serverless → accept recomputation
- ❌ No `F.current_timestamp()` inside `foreachBatch` → use
  `F.lit(datetime.now(timezone.utc))` instead
- ❌ DBFS paths like `/tmp/...` don't work → use `/Volumes/...` (Unity
  Catalog Volumes)
- ❌ Home dir on workers is read-only → models / caches must go to
  Volumes too
- ✅ Catalog/schema management is fine
- ✅ Delta table operations are fine
- ✅ Standard SQL is fine

Started a "Things to NEVER do on Free Edition" list in `docs/decisions.md`
that I'll keep adding to.

## Lessons captured

- **Always use `acks=all` + delivery callbacks** for Kafka producers in
  any context where message loss matters. The performance hit is
  negligible at this volume.
- **`confluent-kafka` over `kafka-python`** — wraps librdkafka, much
  faster, what Confluent officially supports.
- **Don't trust scraped web sources** (yfinance) for anything that needs
  to keep working. Use real APIs with documented contracts (Finnhub).
- **Schema-on-read is unsafe for streaming**. Defining the StructType
  explicitly prevents silent type-coercion bugs that only show up in
  prod.
- **Bronze = append-only audit log, not deduplicated**. Deduplication is
  silver's job. Trying to dedupe in bronze loses data lineage.
- **Free Edition Spark Connect has real teeth**. Read the docs before
  copy-pasting Spark code from random blogs.

## Interview-ready stories

- **yfinance → Finnhub debugging**: 30 minutes of `KeyError`s before
  realizing yfinance scrapes Yahoo's HTML. Switched to a real API with
  documented rate limits. ADR-001.
- **Why Spark Structured Streaming + `availableNow` instead of
  continuous**: most use cases tolerate 5-min delay; continuous streaming
  is 10× the compute for 0.1% of the latency benefit. "Real-time streaming
  ingestion with scheduled micro-batch processing" is the precise way
  to describe it.
- **Bronze/silver/gold separation**: bronze captures raw lineage for
  auditing; silver is where quality contracts get enforced; gold is the
  feature table the agent + dashboard read. Trying to combine layers
  loses one of those three properties.

## Files committed

```
producers/
├── config/stocks.yaml, news.yaml
├── stock_producer.py
├── news_producer.py
└── requirements.txt
databricks/notebooks/
└── 01_kafka_to_bronze.py
docs/
├── decisions.md      (ADR-001 — Finnhub switch)
├── daily.md          (rolling notes)
└── screenshots/
    ├── 01_stock_producer_kafka.png
    └── 02_news_producer_kafka.png
```

Commit messages:
- `block 1 done: finnhub stock producer streaming to kafka`
- `block 2 done: news producer with ticker tagging, dedup, kafka publish`
- `block 3 done: kafka -> bronze delta tables via spark structured streaming`

## What's next

Day 3: silver + gold. `02_bronze_to_silver_stocks` (dedup, derived features
like intraday_range, gap, change_pct via MERGE INTO). `03_bronze_to_silver_news`
(FinBERT sentiment via Pandas UDF, exploded per-ticker rows).
`04_silver_to_gold` (5-min windowed OHLC + price_stddev + sentiment join).
By end of Day 3, gold is the feature table the agent will read in Day 4.