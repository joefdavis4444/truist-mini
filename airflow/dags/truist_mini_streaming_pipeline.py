from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner':            'joe.davis',
    'depends_on_past':  False,
    'email_on_failure': False,
    'retries':          2,
    'retry_delay':      timedelta(minutes=5),
    'retry_exponential_backoff': True,
}

with DAG(
    dag_id='truist_mini_streaming_pipeline',
    description='Streaming silver + gold: processes events landed in S3 bronze',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
    tags=['truist-mini', 'streaming', 'loan-events'],
) as dag:

    # Task 1 — Silver: type normalization + event deduplication
    silver_events = GlueJobOperator(
        task_id='silver_transform_loan_events',
        job_name='truist-mini-silver-loan-events',
        region_name='us-east-1',
        wait_for_completion=True,
        verbose=True,
    )

    # Task 2 — Gold: event activity datamart + summary aggregation
    gold_events = GlueJobOperator(
        task_id='gold_transform_loan_events',
        job_name='truist-mini-gold-loan-events',
        region_name='us-east-1',
        wait_for_completion=True,
        verbose=True,
    )

    silver_events >> gold_events
