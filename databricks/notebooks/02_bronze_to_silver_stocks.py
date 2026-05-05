# Databricks notebook source
# COMMAND ----------
# Cell 1 — imports and config

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from delta.tables import DeltaTable

BRONZE_TABLE = "workspace.pulsetrade.bronze_stock_prices"
SILVER_TABLE = "workspace.pulsetrade.silver_stock_prices"

CHECKPOINT_SILVER = "/Volumes/workspace/pulsetrade/checkpoints/silver_stock_prices"

print(f"bronze: {BRONZE_TABLE}")
print(f"silver: {SILVER_TABLE}")

# COMMAND ----------

# COMMAND ----------
# Cell 2 — create silver_stock_prices table if not exists

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    ticker            STRING NOT NULL,
    price             DOUBLE NOT NULL,
    high              DOUBLE,
    low               DOUBLE,
    open_price        DOUBLE,
    prev_close        DOUBLE,
    quote_timestamp   LONG,
    event_time        TIMESTAMP NOT NULL,
    intraday_range    DOUBLE,
    gap               DOUBLE,
    change_pct        DOUBLE,
    source            STRING,
    silver_processed_at TIMESTAMP
)
USING DELTA
""")

print(f"silver table ready: {SILVER_TABLE}")

# COMMAND ----------

# COMMAND ----------
# Cell 3 — read from bronze and apply silver transformations

bronze = spark.readStream.table(BRONZE_TABLE)

# Window for deduplication: same ticker + same Finnhub quote_timestamp = duplicate.
# Keep the latest one we processed (highest bronze_ingested_at).
dedup_window = Window.partitionBy("ticker", "quote_timestamp") \
                     .orderBy(F.col("bronze_ingested_at").desc())

silver_df = (
    bronze
    # Drop malformed rows where JSON parsing failed (tied to ticker being null)
    .filter(F.col("ticker").isNotNull())
    .filter(F.col("price").isNotNull())

    # Cast ISO-8601 string to proper Timestamp
    .withColumn("event_time", F.to_timestamp("timestamp"))

    # Compute derived features
    .withColumn(
        "intraday_range",
        F.when((F.col("open_price") > 0) & F.col("high").isNotNull() & F.col("low").isNotNull(),
               (F.col("high") - F.col("low")) / F.col("open_price"))
        .otherwise(None)
    )
    .withColumn(
        "gap",
        F.when(F.col("prev_close") > 0,
               (F.col("open_price") - F.col("prev_close")) / F.col("prev_close"))
        .otherwise(None)
    )
    .withColumn(
        "change_pct",
        F.when(F.col("prev_close") > 0,
               (F.col("price") - F.col("prev_close")) / F.col("prev_close"))
        .otherwise(None)
    )

    # Tag when silver processed it
    .withColumn("silver_processed_at", F.current_timestamp())

    # Keep only the silver columns
    .select(
        "ticker", "price", "high", "low", "open_price", "prev_close",
        "quote_timestamp", "event_time",
        "intraday_range", "gap", "change_pct",
        "source", "silver_processed_at",
        # Keep dedup helper temporarily
        "bronze_ingested_at",
    )
)

print("silver dataframe defined")
silver_df.printSchema()

# COMMAND ----------

# COMMAND ----------
# Cell 4 — write to silver_stock_prices using MERGE INTO for idempotency

def merge_to_silver(batch_df, batch_id):
    """
    For each micro-batch:
      1. Deduplicate within the batch (keep latest bronze_ingested_at per quote)
      2. MERGE INTO silver — update if (ticker, quote_timestamp) already exists, insert if not
    
    This handles two duplicate sources:
      - Within-batch: producer retried and sent the same quote twice
      - Cross-batch: notebook re-ran on same Kafka offsets after a checkpoint reset
    """
    if batch_df.isEmpty():
        return

    # Deduplicate within the batch
    deduped = (
        batch_df
        .withColumn("rn", F.row_number().over(dedup_window))
        .filter("rn = 1")
        .drop("rn", "bronze_ingested_at")
    )

    # MERGE INTO silver
    silver = DeltaTable.forName(spark, SILVER_TABLE)
    (
        silver.alias("tgt")
        .merge(
            deduped.alias("src"),
            "tgt.ticker = src.ticker AND tgt.quote_timestamp = src.quote_timestamp"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    
    print(f"batch {batch_id}: merged {deduped.count()} rows")


query = (
    silver_df.writeStream
    .foreachBatch(merge_to_silver)
    .option("checkpointLocation", CHECKPOINT_SILVER)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
print("silver_stock_prices stream complete")

# COMMAND ----------

# COMMAND ----------
# Cell 5 — verify silver_stock_prices

print("=== silver_stock_prices ===")
spark.table(SILVER_TABLE).orderBy(F.col("event_time").desc()).show(5, truncate=False)
print(f"row count: {spark.table(SILVER_TABLE).count()}")

print("\n=== sample query: per-ticker stats ===")
spark.sql(f"""
SELECT
    ticker,
    COUNT(*) AS n_observations,
    ROUND(AVG(intraday_range) * 100, 4) AS avg_intraday_range_pct,
    ROUND(MAX(change_pct) * 100, 2) AS max_change_pct,
    ROUND(MIN(change_pct) * 100, 2) AS min_change_pct
FROM {SILVER_TABLE}
GROUP BY ticker
ORDER BY ticker
""").show()

# COMMAND ----------

# How many bronze rows have null ticker?
bronze_df = spark.table("workspace.pulsetrade.bronze_stock_prices")
print(f"total bronze rows: {bronze_df.count()}")
print(f"rows with NULL ticker: {bronze_df.filter('ticker IS NULL').count()}")
print(f"rows with NULL kafka_value parsing: {bronze_df.filter('price IS NULL').count()}")

# Show me a few null rows to see what's in them
bronze_df.filter('ticker IS NULL').select('kafka_key', 'topic', 'offset', 'price').show(5)