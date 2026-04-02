from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import logging

default_args = {
    'owner':            'joe.davis',
    'depends_on_past':  False,
    'email_on_failure': False,
    'retries':          2,
    'retry_delay':      timedelta(minutes=5),
    'retry_exponential_backoff': True,
}

def check_watermark(**context):
    """Log current watermark state before extraction."""
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('pipeline-watermarks')
    response = table.get_item(Key={'source_table': 'loan_master'})
    item = response.get('Item', {})
    watermark = item.get('last_watermark', 'No watermark — first run')
    last_run = item.get('last_run_ts', 'Never')
    last_count = item.get('last_row_count', 0)
    logging.info(f"Current watermark: {watermark}")
    logging.info(f"Last run: {last_run}")
    logging.info(f"Last row count: {last_count}")
    return watermark

with DAG(
    dag_id='truist_mini_batch_pipeline',
    description='Full batch pipeline: bronze extraction → silver → gold',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
    tags=['truist-mini', 'batch', 'loan-master'],
) as dag:

    # Task 1 — Check watermark before extraction
    check_wm = PythonOperator(
        task_id='check_watermark',
        python_callable=check_watermark,
    )

    # Task 2 — Bronze: incremental extraction from SQLite source
    bronze_extract = GlueJobOperator(
        task_id='bronze_extract_loan_master',
        job_name='truist-mini-loan-master-batch',
        region_name='us-east-1',
        wait_for_completion=True,
        verbose=True,
    )

    # Task 3 — Silver: type normalization + deduplication
    silver_transform = GlueJobOperator(
        task_id='silver_transform_loan_master',
        job_name='truist-mini-silver-loan-master',
        region_name='us-east-1',
        wait_for_completion=True,
        verbose=True,
    )

    # Task 4 — Gold: star schema + CCAR metrics
    gold_transform = GlueJobOperator(
        task_id='gold_transform_loan_master',
        job_name='truist-mini-gold-loan-master',
        region_name='us-east-1',
        wait_for_completion=True,
        verbose=True,
    )

    # DAG dependency chain
    check_wm >> bronze_extract >> silver_transform >> gold_transform
