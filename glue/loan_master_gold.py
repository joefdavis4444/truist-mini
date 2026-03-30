import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timezone

# ── Config ───────────────────────────────────────────────
SILVER_BUCKET = 'truist-mini-silver-jfd'
GOLD_BUCKET   = 'truist-mini-gold-jfd'
REGION        = 'us-east-1'

SILVER_PATH        = f's3://{SILVER_BUCKET}/loan_master/'
FACT_PATH          = f's3://{GOLD_BUCKET}/fact_loan_performance/'
DIM_LOAN_TYPE_PATH = f's3://{GOLD_BUCKET}/dim_loan_type/'
DIM_STATUS_PATH    = f's3://{GOLD_BUCKET}/dim_status/'

# ── Spark Session ────────────────────────────────────────
spark = SparkSession.builder \
    .appName('truist-mini-gold-loan-master') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# ── Step 1: Read silver ──────────────────────────────────
print("[1/5] Reading silver data...")
df_silver = spark.read.parquet(SILVER_PATH)
print(f"      Silver row count: {df_silver.count()}")

# ── Step 2: Build dim_status ─────────────────────────────
print("[2/5] Building dim_status...")
status_data = [
    (1, 'CURRENT',     'Pass',            0.00, 'Performing — no credit concern'),
    (2, 'DELINQUENT',  'Special Mention', 0.05, '30-89 days past due'),
    (3, 'DEFAULT',     'Substandard',     0.20, '90+ days past due'),
    (4, 'PAID_OFF',    'Pass',            0.00, 'Loan fully repaid'),
    (5, 'CHARGED_OFF', 'Loss',            1.00, 'Written off — unrecoverable'),
]
schema = StructType([
    StructField('status_key',         IntegerType(), False),
    StructField('status',             StringType(),  False),
    StructField('occ_classification', StringType(),  False),
    StructField('loss_given_default', FloatType(),   False),
    StructField('description',        StringType(),  True),
])
df_dim_status = spark.createDataFrame(status_data, schema) \
    .withColumn('_dim_status_ts', F.current_timestamp())

df_dim_status.write.mode('overwrite').parquet(DIM_STATUS_PATH)
print(f"      dim_status written: {df_dim_status.count()} rows")

# ── Step 3: Build dim_loan_type ──────────────────────────
print("[3/5] Building dim_loan_type...")
loan_type_data = [
    (1, 'MORTGAGE', 'Real Estate', 'Secured',   True),
    (2, 'AUTO',     'Consumer',    'Secured',   False),
    (3, 'PERSONAL', 'Consumer',    'Unsecured', False),
    (4, 'HELOC',    'Real Estate', 'Secured',   True),
    (5, 'STUDENT',  'Consumer',    'Unsecured', False),
]
schema_lt = StructType([
    StructField('loan_type_key',    IntegerType(), False),
    StructField('loan_type',        StringType(),  False),
    StructField('category',         StringType(),  False),
    StructField('collateral_type',  StringType(),  False),
    StructField('real_estate_flag', BooleanType(), False),
])
df_dim_loan_type = spark.createDataFrame(loan_type_data, schema_lt) \
    .withColumn('_dim_loan_type_ts', F.current_timestamp())

df_dim_loan_type.write.mode('overwrite').parquet(DIM_LOAN_TYPE_PATH)
print(f"      dim_loan_type written: {df_dim_loan_type.count()} rows")

# ── Step 4: Build fact_loan_performance ──────────────────
print("[4/5] Building fact_loan_performance...")

df_dim_status_join    = df_dim_status.drop('_dim_status_ts')
df_dim_loan_type_join = df_dim_loan_type.drop('_dim_loan_type_ts')

df_fact = df_silver \
    .join(df_dim_loan_type_join, on='loan_type', how='left') \
    .join(df_dim_status_join,    on='status',    how='left') \
    .withColumn('report_dt', F.current_date()) \
    .withColumn('months_remaining',
        F.col('term_months') -
        F.months_between(F.current_date(), F.col('origination_dt')).cast(IntegerType())
    ) \
    .withColumn('estimated_balance',
        F.when(F.col('status').isin('PAID_OFF', 'CHARGED_OFF'), F.lit(0.0))
         .otherwise(F.col('principal'))
        .cast(DecimalType(18, 4))
    ) \
    .withColumn('is_delinquent',
        F.when(F.col('status').isin('DELINQUENT', 'DEFAULT'), F.lit(True))
         .otherwise(F.lit(False))
    ) \
    .withColumn('risk_weighted_exposure',
        (F.col('estimated_balance') * F.col('loss_given_default'))
        .cast(DecimalType(18, 4))
    ) \
    .withColumn('_gold_job', F.lit('truist-mini-gold-loan-master')) \
    .withColumn('_gold_ts',  F.current_timestamp()) \
    .select(
        'loan_id', 'customer_id', 'loan_type_key', 'status_key',
        'loan_type', 'status', 'occ_classification',
        'origination_dt', 'report_dt', 'principal', 'estimated_balance',
        'interest_rate', 'term_months', 'months_remaining',
        'is_delinquent', 'loss_given_default', 'risk_weighted_exposure',
        'branch_id', 'MODIFIED_TS', '_gold_job', '_gold_ts'
    )

fact_count = df_fact.count()
print(f"      fact_loan_performance rows: {fact_count}")

# ── Step 5: Write gold ───────────────────────────────────
print("[5/5] Writing fact_loan_performance to gold...")
df_fact.write \
    .mode('overwrite') \
    .partitionBy('occ_classification') \
    .parquet(FACT_PATH)

print(f"\n=== Gold job complete: {fact_count} rows written ===")
spark.stop()
