import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import boto3
from datetime import datetime, timezone

# ── Config ───────────────────────────────────────────────
BRONZE_BUCKET = 'truist-mini-bronze-jfd'
SILVER_BUCKET = 'truist-mini-silver-jfd'
SOURCE_TABLE  = 'loan_master'
REGION        = 'us-east-1'

BRONZE_PATH = f's3://{BRONZE_BUCKET}/batch/core_banking/{SOURCE_TABLE}/'
SILVER_PATH = f's3://{SILVER_BUCKET}/loan_master/'

# ── Spark Session ────────────────────────────────────────
spark = SparkSession.builder \
    .appName('truist-mini-silver-loan-master') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

print(f"[1/5] Reading bronze data from {BRONZE_PATH}...")
df_bronze = spark.read.parquet(BRONZE_PATH)
print(f"      Bronze row count: {df_bronze.count()}")
df_bronze.printSchema()

# ── Step 2: Type normalization ───────────────────────────
print("[2/5] Applying type normalization...")
df_typed = df_bronze \
    .withColumn('loan_id',        F.col('loan_id').cast(StringType())) \
    .withColumn('customer_id',    F.col('customer_id').cast(StringType())) \
    .withColumn('loan_type',      F.col('loan_type').cast(StringType())) \
    .withColumn('origination_dt', F.to_date(F.col('origination_dt'), 'yyyy-MM-dd')) \
    .withColumn('principal',      F.col('principal').cast(DecimalType(18, 4))) \
    .withColumn('interest_rate',  F.col('interest_rate').cast(DecimalType(8, 4))) \
    .withColumn('term_months',    F.col('term_months').cast(IntegerType())) \
    .withColumn('status',         F.col('status').cast(StringType())) \
    .withColumn('branch_id',      F.col('branch_id').cast(StringType())) \
    .withColumn('MODIFIED_TS',    F.to_timestamp(F.col('MODIFIED_TS'), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn('_ingestion_ts',  F.to_timestamp(F.col('_ingestion_ts'))) \
    .withColumn('_silver_ts',     F.current_timestamp())

print("      Type normalization complete.")

# ── Step 3: Deduplication ────────────────────────────────
print("[3/5] Deduplicating records...")
window_spec = Window \
    .partitionBy('loan_id') \
    .orderBy(F.col('MODIFIED_TS').desc())

df_deduped = df_typed \
    .withColumn('row_num', F.row_number().over(window_spec)) \
    .filter(F.col('row_num') == 1) \
    .drop('row_num')

bronze_count = df_typed.count()
silver_count = df_deduped.count()
print(f"      Before dedup: {bronze_count} rows")
print(f"      After dedup:  {silver_count} rows")
print(f"      Duplicates removed: {bronze_count - silver_count}")

# ── Step 4: Add silver metadata ──────────────────────────
print("[4/5] Adding silver metadata...")
df_silver = df_deduped \
    .withColumn('_silver_job',    F.lit('truist-mini-silver-loan-master')) \
    .withColumn('_source_layer',  F.lit('bronze')) \
    .drop('_source_table', '_source_system', '_watermark_start')

# ── Step 5: Write to S3 silver ───────────────────────────
print(f"[5/5] Writing to silver: {SILVER_PATH}...")
df_silver.write \
    .mode('overwrite') \
    .partitionBy('loan_type') \
    .parquet(SILVER_PATH)

print(f"      Silver write complete.")
print(f"\n=== Silver job complete: {silver_count} rows written ===")

spark.stop()
