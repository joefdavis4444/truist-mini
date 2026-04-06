import json
import time
import random
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer
import os

fake = Faker()

BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

EVENT_TYPES = [
    'PAYMENT',
    'MISSED_PAYMENT',
    'ORIGINATION',
    'PAYOFF',
    'DELINQUENCY',
    'STATUS_CHANGE'
]

LOAN_TYPES = ['MORTGAGE', 'AUTO', 'PERSONAL', 'HELOC', 'STUDENT']
STATUSES = ['CURRENT', 'DELINQUENT', 'DEFAULT', 'PAID_OFF', 'CHARGED_OFF']

def generate_loan_event():
    loan_id = f"L{fake.numerify(text='#######')}"
    return {
        'loan_id': loan_id,
        'customer_id': f"C{fake.numerify(text='#####')}",
        'event_type': random.choice(EVENT_TYPES),
        'amount': round(random.uniform(0, 50000), 2),
        'currency': 'USD',
	'loan_type': random.choice(LOAN_TYPES),
	'status': random.choice(STATUSES),
        'account_number': fake.bban(),
        'event_ts': datetime.now(timezone.utc).isoformat(),
        '_ingestion_ts': datetime.now(timezone.utc).isoformat(),
        '_source_topic': 'loan-events-raw',
        '_producer_version': '1.0.0'
    }

print(f"Loan producer started. Connecting to {BOOTSTRAP_SERVERS}...")

while True:
    event = generate_loan_event()
    producer.send(
        topic='loan-events-raw',
        key=event['loan_id'],
        value=event
    )
    producer.flush()
    print(f"Published: {event['event_type']} | loan_id={event['loan_id']} | amount=${event['amount']}")
    time.sleep(1)
