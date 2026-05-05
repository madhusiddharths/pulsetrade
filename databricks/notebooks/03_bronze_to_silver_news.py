# Databricks notebook source
# COMMAND ----------
# Cell 1 — imports and config

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from delta.tables import DeltaTable
import pandas as pd

BRONZE_TABLE = "workspace.pulsetrade.bronze_market_news"
SILVER_TABLE = "workspace.pulsetrade.silver_market_news"
CHECKPOINT_SILVER = "/Volumes/workspace/pulsetrade/checkpoints/silver_market_news"

print(f"bronze: {BRONZE_TABLE}")
print(f"silver: {SILVER_TABLE}")

# COMMAND ----------

# COMMAND ----------
# Cell 2 — create silver_market_news table

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    article_id        STRING NOT NULL,
    ticker            STRING NOT NULL,
    is_primary_ticker BOOLEAN NOT NULL,
    title             STRING,
    description       STRING,
    source_name       STRING,
    url               STRING,
    category          STRING,
    published_at      TIMESTAMP,
    ingested_at       TIMESTAMP,
    sentiment_label   STRING,
    sentiment_score   DOUBLE,
    silver_processed_at TIMESTAMP
)
USING DELTA
""")

print(f"silver table ready: {SILVER_TABLE}")

# COMMAND ----------

# COMMAND ----------
# Cell 3 — install transformers + torch for FinBERT inference
%pip install transformers==4.46.0 torch==2.5.1 --quiet
dbutils.library.restartPython()

# COMMAND ----------

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.pulsetrade.hf_cache")
print("hf_cache volume ready")

# COMMAND ----------

# COMMAND ----------
# Cell 4 — define a Pandas UDF that runs FinBERT inference on a column of text

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

_SENTIMENT_SCHEMA = StructType([
    StructField("label", StringType(), False),
    StructField("score", DoubleType(), False),
])

HF_CACHE_DIR = "/Volumes/workspace/pulsetrade/hf_cache"


@pandas_udf(_SENTIMENT_SCHEMA)
def finbert_sentiment(texts: pd.Series) -> pd.DataFrame:
    import os
    # Cache to a writable Volume (Free Edition home dir is read-only)
    os.environ["HF_HOME"] = HF_CACHE_DIR
    os.environ["HF_HUB_CACHE"] = HF_CACHE_DIR
    os.environ["TRANSFORMERS_CACHE"] = HF_CACHE_DIR
    os.environ["XDG_CACHE_HOME"] = HF_CACHE_DIR

    # Disable Xet downloader — its CAS protocol requires fs features
    # that Unity Catalog Volumes don't expose. Falls back to plain HTTP.
    os.environ["HF_HUB_DISABLE_XET"] = "1"
    os.environ["HF_XET_DISABLE_BACKEND"] = "1"

    from transformers import pipeline

    if not hasattr(finbert_sentiment, "_pipe"):
        finbert_sentiment._pipe = pipeline(
            "sentiment-analysis",
            model="yiyanghkust/finbert-tone",
            tokenizer="yiyanghkust/finbert-tone",
            device=-1,
        )

    pipe = finbert_sentiment._pipe

    safe_texts = texts.fillna("").tolist()
    safe_texts = [t[:1500] for t in safe_texts]

    results = pipe(safe_texts, truncation=True, max_length=512)

    return pd.DataFrame({
        "label": [r["label"].lower() for r in results],
        "score": [float(r["score"]) for r in results],
    })


print("finbert_sentiment UDF registered (xet disabled, cache at", HF_CACHE_DIR + ")")

# COMMAND ----------

# COMMAND ----------
# Cell 5 — silver pipeline: read bronze, explode tickers, score sentiment

bronze = spark.readStream.table(BRONZE_TABLE)

silver_df = (
    bronze
    # Drop malformed rows
    .filter(F.col("article_id").isNotNull())
    .filter(F.col("title").isNotNull())
    .filter(F.size(F.col("tickers")) > 0)
    
    # Cast strings to proper timestamps
    .withColumn("published_at_ts", F.to_timestamp("published_at"))
    .withColumn("ingested_at_ts",  F.to_timestamp("ingested_at"))

    # Each article → multiple rows, one per ticker
    .withColumn("ticker", F.explode("tickers"))
    .withColumn("is_primary_ticker", F.col("ticker") == F.col("primary_ticker"))

    # Run sentiment on title+description
    .withColumn("text_for_sentiment",
                F.concat_ws(". ", F.col("title"), F.coalesce(F.col("description"), F.lit(""))))
    .withColumn("sentiment_struct", finbert_sentiment(F.col("text_for_sentiment")))
    .withColumn("sentiment_label", F.col("sentiment_struct.label"))
    .withColumn("sentiment_score", F.col("sentiment_struct.score"))

    # Tag when silver processed it
    .withColumn("silver_processed_at", F.current_timestamp())

    # Final shape
    .select(
        "article_id",
        "ticker",
        "is_primary_ticker",
        "title",
        "description",
        "source_name",
        "url",
        "category",
        F.col("published_at_ts").alias("published_at"),
        F.col("ingested_at_ts").alias("ingested_at"),
        "sentiment_label",
        "sentiment_score",
        "silver_processed_at",
        "bronze_ingested_at",
    )
)

print("silver dataframe defined")
silver_df.printSchema()

# COMMAND ----------

# COMMAND ----------
# Cell 6 — write to silver_market_news using MERGE INTO

dedup_window = Window.partitionBy("article_id", "ticker") \
                     .orderBy(F.col("bronze_ingested_at").desc())


def merge_news_to_silver(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    deduped = (
        batch_df
        .withColumn("rn", F.row_number().over(dedup_window))
        .filter("rn = 1")
        .drop("rn", "bronze_ingested_at")
    )

    silver = DeltaTable.forName(spark, SILVER_TABLE)
    (
        silver.alias("tgt")
        .merge(
            deduped.alias("src"),
            "tgt.article_id = src.article_id AND tgt.ticker = src.ticker"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    
    print(f"batch {batch_id}: merged {deduped.count()} (article × ticker) rows")


query = (
    silver_df.writeStream
    .foreachBatch(merge_news_to_silver)
    .option("checkpointLocation", CHECKPOINT_SILVER)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
print("silver_market_news stream complete")

# COMMAND ----------

# COMMAND ----------
# Cell 7 — verify silver_market_news

print("=== silver_market_news ===")
(
    spark.table(SILVER_TABLE)
    .select("ticker", "is_primary_ticker", "title", "sentiment_label", "sentiment_score")
    .orderBy(F.col("ingested_at").desc())
    .show(10, truncate=80)
)
print(f"row count: {spark.table(SILVER_TABLE).count()}")


print("\n=== sentiment distribution per ticker ===")
spark.sql(f"""
SELECT
    ticker,
    sentiment_label,
    COUNT(*) AS n,
    ROUND(AVG(sentiment_score), 3) AS avg_score
FROM {SILVER_TABLE}
WHERE is_primary_ticker = TRUE
GROUP BY ticker, sentiment_label
ORDER BY ticker, sentiment_label
""").show()