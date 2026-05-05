# Databricks notebook source
# COMMAND ----------
# Cell 1 — imports and config

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

SILVER_STOCK    = "workspace.pulsetrade.silver_stock_prices"
SILVER_NEWS     = "workspace.pulsetrade.silver_market_news"
GOLD_TABLE      = "workspace.pulsetrade.gold_5min_features"

CHECKPOINT_GOLD = "/Volumes/workspace/pulsetrade/checkpoints/gold_5min_features"

# 5-minute aggregation window
WINDOW_DURATION = "5 minutes"

# Watermark: how late we accept events (drops events older than this from current time)
# 30 min covers the case where a producer was offline briefly and is catching up
WATERMARK_DELAY = "30 minutes"

print(f"silver stock: {SILVER_STOCK}")
print(f"silver news:  {SILVER_NEWS}")
print(f"gold:         {GOLD_TABLE}")

# COMMAND ----------

# COMMAND ----------
# Cell 2 — create gold_5min_features table

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
    ticker                STRING NOT NULL,
    window_start          TIMESTAMP NOT NULL,
    window_end            TIMESTAMP NOT NULL,
    n_observations        BIGINT,
    open_5min             DOUBLE,
    close_5min            DOUBLE,
    high_5min             DOUBLE,
    low_5min              DOUBLE,
    mean_price            DOUBLE,
    price_stddev          DOUBLE,
    mean_change_pct       DOUBLE,
    mean_intraday_range   DOUBLE,
    mean_news_sentiment   DOUBLE,
    news_article_count    BIGINT,
    gold_processed_at     TIMESTAMP
)
USING DELTA
""")

print(f"gold table ready: {GOLD_TABLE}")

# COMMAND ----------

# COMMAND ----------plan 2 for cell 3
# Cell 3 — define a streaming source but do windowing inside foreachBatch
# This avoids streaming watermark complexity for small data volumes.

# Just stream silver as-is; we'll aggregate inside foreachBatch
stock_silver_stream = spark.readStream.table(SILVER_STOCK)

print("stock streaming source defined (windowing happens in foreachBatch)")

# COMMAND ----------

# COMMAND ----------plan 2 for cell 5
# Cell 5 — write gold using batch-style windowing inside foreachBatch

def write_gold(batch_df, batch_id):
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable
    from datetime import datetime, timezone

    if batch_df.isEmpty():
        print(f"batch {batch_id}: no new silver data")
        return

    # Window the batch's data into 5-minute buckets
    stock_5min = (
        batch_df
        .filter(F.col("event_time").isNotNull())
        .groupBy(
            F.col("ticker"),
            F.window(F.col("event_time"), WINDOW_DURATION).alias("window")
        )
        .agg(
            F.count("*").alias("n_observations"),
            F.first("price", ignorenulls=True).alias("open_5min"),
            F.last("price", ignorenulls=True).alias("close_5min"),
            F.max("price").alias("high_5min"),
            F.min("price").alias("low_5min"),
            F.avg("price").alias("mean_price"),
            F.stddev("price").alias("price_stddev"),
            F.avg("change_pct").alias("mean_change_pct"),
            F.avg("intraday_range").alias("mean_intraday_range"),
        )
        .select(
            "ticker",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "n_observations",
            "open_5min", "close_5min", "high_5min", "low_5min",
            "mean_price", "price_stddev",
            "mean_change_pct", "mean_intraday_range",
        )
    )

    stock_count = stock_5min.count()
    if stock_count == 0:
        print(f"batch {batch_id}: no windows formed")
        return

    # Same news join as before
    window_bounds = stock_5min.agg(
        F.min("window_start").alias("min_ws"),
        F.max("window_end").alias("max_we"),
    ).collect()[0]

    news_for_windows = (
        spark.table(SILVER_NEWS)
        .filter(F.col("ingested_at").between(window_bounds["min_ws"], window_bounds["max_we"]))
        .groupBy(
            F.col("ticker"),
            F.window(F.col("ingested_at"), WINDOW_DURATION).alias("window")
        )
        .agg(
            F.avg("sentiment_score").alias("mean_news_sentiment"),
            F.count("*").alias("news_article_count"),
        )
        .select(
            "ticker",
            F.col("window.start").alias("window_start"),
            "mean_news_sentiment",
            "news_article_count",
        )
    )

    joined = (
        stock_5min.alias("s")
        .join(
            news_for_windows.alias("n"),
            (F.col("s.ticker") == F.col("n.ticker")) &
            (F.col("s.window_start") == F.col("n.window_start")),
            "left"
        )
        .select(
            F.col("s.ticker").alias("ticker"),
            F.col("s.window_start").alias("window_start"),
            F.col("s.window_end").alias("window_end"),
            F.col("s.n_observations").alias("n_observations"),
            "open_5min", "close_5min", "high_5min", "low_5min",
            "mean_price", "price_stddev",
            "mean_change_pct", "mean_intraday_range",
            "mean_news_sentiment", "news_article_count",
            F.lit(datetime.now(timezone.utc)).alias("gold_processed_at"),
        )
    )

    # MERGE INTO gold
    if not spark.catalog.tableExists(GOLD_TABLE):
        joined.write.format("delta").mode("append").saveAsTable(GOLD_TABLE)
        print(f"batch {batch_id}: gold table didn't exist, created with {stock_count} windows")
    else:
        gold = DeltaTable.forName(spark, GOLD_TABLE)
        (
            gold.alias("tgt")
            .merge(
                joined.alias("src"),
                "tgt.ticker = src.ticker AND tgt.window_start = src.window_start"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"batch {batch_id}: merged {stock_count} (ticker × window) rows into gold")


query = (
    stock_silver_stream.writeStream
    .foreachBatch(write_gold)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_GOLD)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
print("gold_5min_features stream complete")

# COMMAND ----------

# COMMAND ----------
# Cell 6 — verify gold_5min_features

print("=== gold_5min_features ===")
(
    spark.table(GOLD_TABLE)
    .orderBy(F.col("window_start").desc(), "ticker")
    .show(15, truncate=False)
)
print(f"row count: {spark.table(GOLD_TABLE).count()}")


print("\n=== sample query: per-ticker latest window ===")
spark.sql(f"""
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY window_start DESC) AS rn
    FROM {GOLD_TABLE}
)
SELECT ticker, window_start, window_end,
       ROUND(open_5min, 2) AS open,
       ROUND(close_5min, 2) AS close,
       ROUND(high_5min, 2) AS high,
       ROUND(low_5min, 2) AS low,
       n_observations,
       ROUND(price_stddev, 4) AS price_stddev,
       news_article_count,
       ROUND(mean_news_sentiment, 3) AS mean_news_sentiment
FROM ranked
WHERE rn = 1
ORDER BY ticker
""").show(truncate=False)