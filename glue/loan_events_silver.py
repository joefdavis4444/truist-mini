import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

# ── Config ───────────────────────────────────────────────
BRONZE_BUCKET = 'truist-mini-bronze-jfd'
SILVER_BUCKET = 'truist-mini-silver-jfd'

BRONZE_PATH = f's3://{BRONZE_BUCKET}/topics/loan-events-raw/'
SILVER_PATH = f's3://{SILVER_BUCKET}/loan_events/'

# ── Spark Session ────────────────────────────────────────
spark = SparkSession.builder \
    .appName('truist-mini-silver-loan-events') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# ── Step 1: Read bronze NDJSON ───────────────────────────
print("[1/4] Reading bronze streaming events...")
df_bronze = spark.read.json(BRONZE_PATH)
print(f"      Bronze row count: {df_bronze.count()}")
df_bronze.printSchema()

# ── Step 2: Type normalization ───────────────────────────
print("[2/4] Applying type normalization...")
df_typed = df_bronze \
    .withColumn('loan_id',       F.col('loan_id').cast(StringType())) \
    .withColumn('customer_id',   F.col('customer_id').cast(StringType())) \
    .withColumn('event_type',    F.col('event_type').cast(StringType())) \
    .withColumn('amount',        F.col('amount').cast(DecimalType(18, 4))) \
    .withColumn('currency',      F.col('currency').cast(StringType())) \
    .withColumn('account_number',F.col('account_number').cast(StringType())) \
    .withColumn('event_ts',      F.to_timestamp(F.col('event_ts'))) \
    .withColumn('_ingestion_ts', F.to_timestamp(F.col('_ingestion_ts'))) \
    .withColumn('_partition',    F.col('_partition').cast(IntegerType())) \
    .withColumn('_offset',       F.col('_offset').cast(LongType())) \
    .withColumn('_silver_ts',    F.current_timestamp())

# ── Step 3: Deduplication ────────────────────────────────
print("[3/4] Deduplicating events...")
window_spec = Window \
    .partitionBy('loan_id', 'event_ts', 'event_type') \
    .orderBy(F.col('_ingestion_ts').desc())

df_deduped = df_typed \
    .withColumn('row_num', F.row_number().over(window_spec)) \
    .filter(F.col('row_num') == 1) \
    .drop('row_num')

bronze_count = df_typed.count()
silver_count = df_deduped.count()
print(f"      Before dedup: {bronze_count} rows")
print(f"      After dedup:  {silver_count} rows")
print(f"      Duplicates removed: {bronze_count - silver_count}")

# ── Step 4: Write to silver ──────────────────────────────
print("[4/4] Writing to silver...")
df_silver = df_deduped \
    .withColumn('_silver_job',   F.lit('truist-mini-silver-loan-events')) \
    .withColumn('_source_layer', F.lit('bronze')) \
    .drop('_source_topic', '_producer_version')

df_silver.write \
    .mode('overwrite') \
    .partitionBy('event_type') \
    .parquet(SILVER_PATH)

print(f"\n=== Streaming silver complete: {silver_count} rows written ===")
spark.stop()
