import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ── Config ───────────────────────────────────────────────
SILVER_BUCKET = 'truist-mini-silver-jfd'
GOLD_BUCKET   = 'truist-mini-gold-jfd'

SILVER_EVENTS_PATH  = f's3://{SILVER_BUCKET}/loan_events/'
SILVER_MASTER_PATH  = f's3://{SILVER_BUCKET}/loan_master/'
FACT_ACTIVITY_PATH  = f's3://{GOLD_BUCKET}/fact_loan_event_activity/'
FACT_SUMMARY_PATH   = f's3://{GOLD_BUCKET}/fact_event_summary/'

# ── Spark Session ────────────────────────────────────────
spark = SparkSession.builder \
    .appName('truist-mini-gold-loan-events') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# ── Step 1: Read silver ──────────────────────────────────
print("[1/5] Reading silver data...")
df_events = spark.read.parquet(SILVER_EVENTS_PATH)
df_master = spark.read.parquet(SILVER_MASTER_PATH)
print(f"      Events: {df_events.count()} rows")
print(f"      Loan master: {df_master.count()} rows")

# ── Step 2: Prepare master for join ─────────────────────
print("[2/5] Preparing loan master for join...")
df_master_slim = df_master.select(
    'loan_id',
    'loan_type',
    'principal',
    'interest_rate',
    'term_months',
    'status',
    'branch_id',
    'origination_dt'
).withColumnRenamed('status',    'current_status') \
 .withColumnRenamed('loan_type', 'loan_type')

# ── Step 3: Build fact_loan_event_activity ───────────────
print("[3/5] Building fact_loan_event_activity...")
df_activity = df_events \
    .join(df_master_slim, on='loan_id', how='left') \
    .withColumn('event_date',    F.to_date(F.col('event_ts'))) \
    .withColumn('event_year',    F.year(F.col('event_ts'))) \
    .withColumn('event_month',   F.month(F.col('event_ts'))) \
    .withColumn('is_adverse',
        F.when(F.col('event_type').isin(
            'MISSED_PAYMENT', 'DELINQUENCY', 'STATUS_CHANGE'), F.lit(True))
         .otherwise(F.lit(False))
    ) \
    .withColumn('payment_flag',
        F.when(F.col('event_type').isin('PAYMENT', 'PAYOFF'), F.lit(True))
         .otherwise(F.lit(False))
    ) \
    .withColumn('_gold_job', F.lit('truist-mini-gold-loan-events')) \
    .withColumn('_gold_ts',  F.current_timestamp()) \
    .select(
        'loan_id',
        'customer_id',
        'event_type',
        'event_ts',
        'event_date',
        'event_year',
        'event_month',
        'amount',
        'currency',
        'loan_type',
        'principal',
        'interest_rate',
        'term_months',
        'current_status',
        'branch_id',
        'origination_dt',
        'is_adverse',
        'payment_flag',
        '_partition',
        '_offset',
        '_gold_job',
        '_gold_ts'
    )

activity_count = df_activity.count()
print(f"      fact_loan_event_activity rows: {activity_count}")

df_activity.write \
    .mode('overwrite') \
    .partitionBy('event_type') \
    .parquet(FACT_ACTIVITY_PATH)

print(f"      Written to gold partitioned by event_type")

# ── Step 4: Build fact_event_summary (aggregated) ────────
print("[4/5] Building fact_event_summary...")
df_summary = df_activity \
    .groupBy(
        'event_type',
        'loan_type',
        'branch_id',
        'event_year',
        'event_month',
        'is_adverse'
    ) \
    .agg(
        F.count('*').alias('event_count'),
        F.sum('amount').cast(DecimalType(18, 4)).alias('total_amount'),
        F.avg('amount').cast(DecimalType(18, 4)).alias('avg_amount'),
        F.countDistinct('loan_id').alias('distinct_loans'),
        F.countDistinct('customer_id').alias('distinct_customers'),
        F.min('event_ts').alias('first_event_ts'),
        F.max('event_ts').alias('last_event_ts')
    ) \
    .withColumn('_gold_job', F.lit('truist-mini-gold-loan-events')) \
    .withColumn('_gold_ts',  F.current_timestamp())

summary_count = df_summary.count()
print(f"      fact_event_summary rows: {summary_count}")

df_summary.write \
    .mode('overwrite') \
    .partitionBy('event_type') \
    .parquet(FACT_SUMMARY_PATH)

print(f"      Written to gold partitioned by event_type")

# ── Step 5: Print sample metrics ─────────────────────────
print("[5/5] Sample summary metrics:")
df_summary.groupBy('event_type') \
    .agg(
        F.sum('event_count').alias('total_events'),
        F.sum('total_amount').alias('total_volume')
    ) \
    .orderBy('event_type') \
    .show(truncate=False)

print(f"\n=== Events gold complete ===")
print(f"    fact_loan_event_activity: {activity_count} rows")
print(f"    fact_event_summary:       {summary_count} rows")
spark.stop()
