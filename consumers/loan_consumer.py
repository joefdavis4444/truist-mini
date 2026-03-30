import json
import os
import boto3
from datetime import datetime, timezone
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
BRONZE_BUCKET = os.environ.get('BRONZE_BUCKET', 'truist-mini-bronze-jfd')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

s3 = boto3.client('s3', region_name=AWS_REGION)

consumer = KafkaConsumer(
    'loan-events-raw',
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    group_id='loan-consumer-group',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print(f"Loan consumer started. Reading from loan-events-raw, writing to s3://{BRONZE_BUCKET}...")

def get_s3_key(event):
    now = datetime.now(timezone.utc)
    return (
        f"topics/loan-events-raw/"
        f"year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"loan_{event['loan_id']}_{now.timestamp():.0f}.json"
    )

for message in consumer:
    event = message.value
    event['_partition'] = message.partition
    event['_offset'] = message.offset
    event['_pod_name'] = os.environ.get('POD_NAME', 'local')

    s3_key = get_s3_key(event)

    try:
        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key=s3_key,
            Body=json.dumps(event).encode('utf-8'),
            ContentType='application/json'
        )
        consumer.commit()
        print(f"Written: s3://{BRONZE_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"ERROR writing to S3: {e} — offset NOT committed")
