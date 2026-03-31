-- Truist Mini Platform — Athena DDL
-- Run these in order to register gold tables in the Glue Catalog

-- 1. fact_loan_performance (batch datamart — credit risk / CCAR)
CREATE EXTERNAL TABLE IF NOT EXISTS truist_mini.fact_loan_performance (
    loan_id STRING, customer_id STRING, loan_type_key INT, status_key INT,
    loan_type STRING, status STRING, origination_dt DATE, report_dt DATE,
    principal DECIMAL(18,4), estimated_balance DECIMAL(18,4),
    interest_rate DECIMAL(8,4), term_months INT, months_remaining INT,
    is_delinquent BOOLEAN, loss_given_default FLOAT,
    risk_weighted_exposure DECIMAL(18,4), branch_id STRING,
    modified_ts TIMESTAMP, gold_job STRING, gold_ts TIMESTAMP
)
PARTITIONED BY (occ_classification STRING)
STORED AS PARQUET
LOCATION 's3://truist-mini-gold-jfd/fact_loan_performance/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- 2. fact_loan_event_activity (streaming datamart — row level events)
CREATE EXTERNAL TABLE IF NOT EXISTS truist_mini.fact_loan_event_activity (
    loan_id STRING, customer_id STRING, event_ts TIMESTAMP,
    event_date DATE, event_year INT, event_month INT,
    amount DECIMAL(18,4), currency STRING, loan_type STRING,
    principal DECIMAL(18,4), interest_rate DECIMAL(8,4), term_months INT,
    current_status STRING, branch_id STRING, origination_dt DATE,
    is_adverse BOOLEAN, payment_flag BOOLEAN,
    kafka_partition INT, kafka_offset BIGINT, gold_job STRING, gold_ts TIMESTAMP
)
PARTITIONED BY (event_type STRING)
STORED AS PARQUET
LOCATION 's3://truist-mini-gold-jfd/fact_loan_event_activity/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- 3. fact_event_summary (streaming datamart — aggregated metrics)
CREATE EXTERNAL TABLE IF NOT EXISTS truist_mini.fact_event_summary (
    loan_type STRING, branch_id STRING, event_year INT, event_month INT,
    is_adverse BOOLEAN, event_count BIGINT, total_amount DECIMAL(18,4),
    avg_amount DECIMAL(18,4), distinct_loans BIGINT,
    distinct_customers BIGINT, first_event_ts TIMESTAMP,
    last_event_ts TIMESTAMP, gold_job STRING, gold_ts TIMESTAMP
)
PARTITIONED BY (event_type STRING)
STORED AS PARQUET
LOCATION 's3://truist-mini-gold-jfd/fact_event_summary/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- 4. dim_status (OCC risk classification)
CREATE EXTERNAL TABLE IF NOT EXISTS truist_mini.dim_status (
    status_key INT, status STRING, occ_classification STRING,
    loss_given_default FLOAT, description STRING
)
STORED AS PARQUET
LOCATION 's3://truist-mini-gold-jfd/dim_status/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- 5. dim_loan_type
CREATE EXTERNAL TABLE IF NOT EXISTS truist_mini.dim_loan_type (
    loan_type_key INT, loan_type STRING, category STRING,
    collateral_type STRING, real_estate_flag BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://truist-mini-gold-jfd/dim_loan_type/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- Run after table creation to register partitions
MSCK REPAIR TABLE truist_mini.fact_loan_performance;
MSCK REPAIR TABLE truist_mini.fact_loan_event_activity;
MSCK REPAIR TABLE truist_mini.fact_event_summary;
