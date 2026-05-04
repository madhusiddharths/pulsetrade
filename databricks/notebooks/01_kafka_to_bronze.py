# Databricks notebook source
# COMMAND ----------
# Cell 1 — imports and configuration

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, IntegerType, ArrayType
)

# Pull Kafka credentials from Databricks Secrets (set up via CLI before running)
KAFKA_BOOTSTRAP = dbutils.secrets.get(scope="pulsetrade", key="kafka-bootstrap-servers")
KAFKA_KEY = dbutils.secrets.get(scope="pulsetrade", key="kafka-api-key")
KAFKA_SECRET = dbutils.secrets.get(scope="pulsetrade", key="kafka-api-secret")

# Bronze table identifiers — Free Edition uses the default catalog
BRONZE_STOCK_TABLE = "workspace.pulsetrade.bronze_stock_prices"
BRONZE_NEWS_TABLE  = "workspace.pulsetrade.bronze_market_news"

# Checkpoint paths track which Kafka offsets have been read.
# Without these, every run would re-read the entire topic from offset 0.
CHECKPOINT_STOCK = "/Volumes/workspace/pulsetrade/checkpoints/bronze_stock_prices"
CHECKPOINT_NEWS  = "/Volumes/workspace/pulsetrade/checkpoints/bronze_market_news"

print(f"Kafka bootstrap: {KAFKA_BOOTSTRAP}")
print(f"Kafka key (first 8 chars): {KAFKA_KEY[:8]}...")
print(f"Bronze stock table: {BRONZE_STOCK_TABLE}")
print(f"Bronze news table:  {BRONZE_NEWS_TABLE}")

# COMMAND ----------

# COMMAND ----------
# Cell 2 — helper: build a Spark Kafka reader for Confluent Cloud

def kafka_reader(topic: str):
    """
    Returns a Spark streaming DataFrameReader configured for Confluent Cloud.

    Uses SASL_SSL with PLAIN auth — same protocol as your local Python producers.
    `startingOffsets="earliest"` means on first run we read all messages in the
    topic; subsequent runs use the checkpoint to resume from where we left off.
    """
    jaas = (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        'required '
        f'username="{KAFKA_KEY}" '
        f'password="{KAFKA_SECRET}";'
    )
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

print("kafka_reader helper defined")

# COMMAND ----------

# COMMAND ----------
# Cell 3 — JSON schemas matching what producers send

# Matches StockTick dataclass in producers/stock_producer.py
stock_schema = StructType([
    StructField("ticker",          StringType(), False),
    StructField("price",           DoubleType(), False),
    StructField("high",            DoubleType(), True),
    StructField("low",             DoubleType(), True),
    StructField("open_price",      DoubleType(), True),
    StructField("prev_close",      DoubleType(), True),
    StructField("timestamp",       StringType(), False),  # ISO 8601 string from producer
    StructField("quote_timestamp", LongType(),   True),
    StructField("source",          StringType(), True),
])

# Matches NewsArticle dataclass in producers/news_producer.py
news_schema = StructType([
    StructField("article_id",      StringType(), False),
    StructField("title",           StringType(), False),
    StructField("description",     StringType(), True),
    StructField("source_name",     StringType(), True),
    StructField("url",             StringType(), False),
    StructField("published_at",    StringType(), True),
    StructField("ingested_at",     StringType(), False),
    StructField("category",        StringType(), True),
    StructField("tickers",         ArrayType(StringType()), True),
    StructField("primary_ticker",  StringType(), True),
    StructField("source",          StringType(), True),
])

print("schemas defined")

# COMMAND ----------

# COMMAND ----------
# Cell 3.5 — create a Unity Catalog Volume for streaming checkpoints
# Free Edition has DBFS disabled, so checkpoint paths must live under
# /Volumes/<catalog>/<schema>/<volume>/...

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.pulsetrade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.pulsetrade.checkpoints")

print("volume ready: /Volumes/workspace/pulsetrade/checkpoints")

# COMMAND ----------

# COMMAND ----------
# Cell 4 — stream stock-prices Kafka → bronze_stock_prices Delta table

stock_raw = kafka_reader("stock-prices")

stock_parsed = (
    stock_raw
    .selectExpr("CAST(key AS STRING) AS kafka_key",
                "CAST(value AS STRING) AS kafka_value",
                "topic", "partition", "offset", "timestamp AS kafka_timestamp")
    .select(
        col("kafka_key"),
        col("kafka_timestamp"),
        col("topic"),
        col("partition"),
        col("offset"),
        from_json(col("kafka_value"), stock_schema).alias("data"),
    )
    .select(
        col("kafka_key"),
        col("kafka_timestamp"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("data.*"),                           # flatten JSON fields
        current_timestamp().alias("bronze_ingested_at"),
    )
)

(
    stock_parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_STOCK)
    .trigger(availableNow=True)
    .toTable(BRONZE_STOCK_TABLE)
)

print(f"stock-prices stream started → {BRONZE_STOCK_TABLE}")

# COMMAND ----------

# COMMAND ----------
# Cell 5 — stream market-news Kafka → bronze_market_news Delta table

news_raw = kafka_reader("market-news")

news_parsed = (
    news_raw
    .selectExpr("CAST(key AS STRING) AS kafka_key",
                "CAST(value AS STRING) AS kafka_value",
                "topic", "partition", "offset", "timestamp AS kafka_timestamp")
    .select(
        col("kafka_key"),
        col("kafka_timestamp"),
        col("topic"),
        col("partition"),
        col("offset"),
        from_json(col("kafka_value"), news_schema).alias("data"),
    )
    .select(
        col("kafka_key"),
        col("kafka_timestamp"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("data.*"),
        current_timestamp().alias("bronze_ingested_at"),
    )
)

(
    news_parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_NEWS)
    .trigger(availableNow=True)
    .toTable(BRONZE_NEWS_TABLE)
)

print(f"market-news stream started → {BRONZE_NEWS_TABLE}")

# COMMAND ----------

# COMMAND ----------
# Cell 6 — verification: row counts and a sample of each bronze table

print("=== bronze_stock_prices ===")
spark.table(BRONZE_STOCK_TABLE).show(5, truncate=False)
print(f"row count: {spark.table(BRONZE_STOCK_TABLE).count()}")

print("\n=== bronze_market_news ===")
spark.table(BRONZE_NEWS_TABLE).show(5, truncate=False)
print(f"row count: {spark.table(BRONZE_NEWS_TABLE).count()}")