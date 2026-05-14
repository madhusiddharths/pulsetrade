# Day 3 — Silver + gold layers (✅ complete)

## What I built

The hardest day so far. Three Spark notebooks chained: bronze → silver
(stocks), bronze → silver (news with FinBERT sentiment), silver → gold
(5-min windowed features). Two of the three notebooks worked first try;
the news one had a multi-layered bug that took ~2 hours to fully resolve.

### Notebook 02 — Bronze stocks → Silver stocks

`02_bronze_to_silver_stocks.py`. Reads `bronze_stock_prices`, dedupes,
computes derived features, MERGE INTOs `silver_stock_prices`.

Derived features added per row:
- `intraday_range` = `high - low`
- `gap` = `open - prev_close`
- `change_pct` = `(price - prev_close) / prev_close`

Dedup strategy: `row_number() OVER (PARTITION BY ticker, quote_timestamp
ORDER BY bronze_ingested_at DESC)`, keep `rn = 1`. This handles the case
where a Kafka message gets reprocessed (e.g., Spark replays from an
earlier offset after a checkpoint mismatch).

MERGE INTO pattern:
```python
silver.alias("tgt").merge(
    deduped.alias("src"),
    "tgt.ticker = src.ticker AND tgt.quote_timestamp = src.quote_timestamp"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

This is the "upsert" pattern. New rows insert; matches update (which
handles the case where prices for the same timestamp get corrected later).
Idempotent — running the notebook twice doesn't double-count.

Ran fine first try.

### Notebook 03 — Bronze news → Silver news (with FinBERT)

This was the long one. The flow:
1. Read bronze_market_news
2. Explode the `tickers` array (one article → one row per ticker mentioned)
3. Run FinBERT sentiment classification on `title + description`
4. MERGE into `silver_market_news` keyed on `(article_id, ticker)`

The FinBERT integration is where Free Edition restrictions came home to
roost. The model is from HuggingFace, ~440MB of weights, needs to be
loaded inside a Pandas UDF that runs on workers.

#### 3-layer FinBERT-on-Free-Edition debugging session

**Layer 1**: read-only home dir on workers. HuggingFace defaults to caching
the model at `~/.cache/huggingface/`. On Free Edition serverless workers,
that path is read-only. Solution: redirect every relevant env var
**inside** the UDF (driver env vars don't propagate to workers):

```python
os.environ["HF_HOME"] = "/Volumes/workspace/pulsetrade/hf_cache"
os.environ["HF_HUB_CACHE"] = "/Volumes/workspace/pulsetrade/hf_cache"
os.environ["TRANSFORMERS_CACHE"] = "/Volumes/workspace/pulsetrade/hf_cache"
os.environ["XDG_CACHE_HOME"] = "/Volumes/workspace/pulsetrade/hf_cache"
```

**Layer 2**: Xet protocol error. HF's new Xet downloader uses content-
addressable storage features (CAS) that Unity Catalog Volumes don't
expose. Got `CAS service error: os error 95`. Disabled with:

```python
os.environ["HF_HUB_DISABLE_XET"] = "1"
os.environ["HF_XET_DISABLE_BACKEND"] = "1"
```

Falls back to plain HTTPS download. Slower, but works.

**Layer 3**: `transformers` module not found. The Free Edition serverless
worker image doesn't include `transformers`. Initially I'd assumed the
Databricks-provided ML runtime would have it; Free Edition serverless does
not. Fix: `%pip install transformers torch` at the top of the notebook +
`dbutils.library.restartPython()`. (This came up again on Day 7 when I
ran the notebook on a fresh session — added the install cell permanently
at the top.)

After all three layers fixed, the UDF worked: 100 articles processed
in ~12 seconds (mostly model loading; per-article inference is
sub-100ms).

#### Architecture decision — pre-download on driver, share to workers

Final pattern I settled on:
1. Driver pre-downloads the model snapshot to the Volume
2. UDF reads from the local Volume path (not HF Hub) on workers

This avoids every worker re-downloading 440MB. Standard production
pattern — load once, share via shared storage. Documented in
`docs/decisions.md`.

### Notebook 04 — Silver → Gold (5-min windowed features)

Final layer: aggregate silver into the feature table the agent + dashboard
will both consume. Schema:

```sql
gold_5min_features (
    ticker, window_start, window_end,
    open_5min, close_5min, high_5min, low_5min,
    mean_price, price_stddev,
    mean_change_pct, mean_intraday_range,
    mean_news_sentiment, news_article_count,
    n_observations, gold_processed_at
)
```

One row per `(ticker, 5-min window)`. Joins silver stocks with silver news
on `(ticker, window)`.

#### Plan 1 — streaming windowed aggregation → broken

First implementation: `groupBy(window(event_time, "5 minutes"))` with
watermarks. The right pattern in theory.

**Problem**: low-volume gold means watermarks rarely advance enough to
emit results. Cell 6 returned 0 rows even though silver had data. After
~30 min of poking at watermark settings, decided this was wrong
architecturally — streaming aggregation is overkill for ~5 windows-per-hour
volume.

Also hit `STREAMING_STATEFUL_OPERATOR_NOT_MATCH_IN_STATE_METADATA` when
I tried to switch from one aggregation to another mid-stream. The Spark
checkpoint remembered the previous aggregation's state shape.

#### Plan 2 — batch-windowing inside `foreachBatch`

Rewrote: stream is just "read silver" (stateless); aggregation happens
inside `foreachBatch` as a normal batch operation. Each micro-batch:
1. Get new silver rows
2. Group by `(ticker, window(event_time, "5 minutes"))`
3. Compute aggregates
4. Join with news aggregates for the same window
5. MERGE INTO gold

Worked. Cell 6 returned rows for all 5 tickers within ~5 min of starting.

Lesson: streaming-stateful-operations are powerful but expensive in
checkpoint complexity. For low-volume aggregations, batch-windowing inside
`foreachBatch` is cleaner.

#### Checkpoint-mismatch story

When I switched from Plan 1 to Plan 2, the old checkpoint had metadata
saying "this stream has stateful operators." Plan 2 doesn't. Streaming
checkpoint refused to start. Fix: `dbutils.fs.rm(CHECKPOINT_GOLD,
recurse=True)` to wipe and start fresh. Gold table itself is fine because
`foreachBatch` does idempotent MERGE.

This same fix came up again on Day 6 when upstream silver got a MERGE
that broke a different stream's append-only assumption. The pattern
("upstream changed, downstream checkpoint stuck, wipe checkpoint") is
worth knowing.

## Lessons captured

- **Driver env vars don't propagate to workers in Pandas UDFs**. Anything
  the UDF depends on (cache paths, API keys, feature flags) needs to be
  set *inside* the UDF body, not at the notebook top.
- **HuggingFace Xet downloader fails on UC Volumes**. Always disable it
  on Databricks Free Edition: `HF_HUB_DISABLE_XET=1`.
- **Pre-download large models on driver, point workers at local paths**.
  Production pattern; avoids N workers re-downloading the same weights.
- **Streaming watermarks are an emission delay**, not a filter. Low-volume
  aggregations may never emit because watermarks never advance enough.
- **`foreachBatch` is the escape hatch** when you need full DataFrame
  operations inside a streaming pipeline. Trade-off: you lose end-to-end
  exactly-once semantics, but MERGE INTO gives you idempotent writes
  which is often what you actually need.
- **Checkpoint metadata remembers stateful-operator shape**. Switching a
  stream from stateful to stateless (or vice versa) requires wiping the
  checkpoint.

## Interview-ready stories

- **3-layer FinBERT debugging on Free Edition**: read-only fs → env var
  propagation, Xet protocol incompatibility, missing transformers package.
  Each was the only issue I could see at the time; only after fixing one
  did the next surface. Documented as a sequence in `docs/decisions.md`.
- **Streaming → batch-windowing architectural switch**: Plan 1 was the
  "correct" streaming pattern but wrong for low-volume data. Plan 2
  trades exactly-once streaming semantics for `foreachBatch` + idempotent
  MERGE. Same correctness, much simpler.
- **MERGE INTO as the upsert pattern**: silver is mutable (corrections
  arrive); MERGE on the natural key (`ticker + quote_timestamp` for
  stocks, `article_id + ticker` for news). Idempotent re-runs are free.
- **Free Edition Spark Connect restrictions**: serverless is more
  restrictive than full Spark. Maintained a "never-do" list in
  `decisions.md` so I'd remember next time.

## Files committed

```
databricks/notebooks/
├── 02_bronze_to_silver_stocks.py
├── 03_bronze_to_silver_news.py
└── 04_silver_to_gold.py
docs/
├── decisions.md         (added 3 ADRs today)
├── daily.md             (FinBERT debugging story)
└── screenshots/
    └── 05_gold_5min_features.png
```

Commit message:
`day 3 complete: silver + gold layers with sentiment + windowed features`

## Cost at end of day 3

Still ~$5 (Anthropic top-up, mostly unused). Free Edition Databricks
compute is free. Confluent is on $400 credit. Gemini is free tier.

## What's next

Day 4: step out of Databricks. Build `api/` service — FastAPI + LangGraph
agent + Gemini reasoning + Postgres for investigation storage. The agent
will read from gold (the table built today) and write markdown briefs.
This is where it stops looking like a data pipeline and starts looking
like an AI product.