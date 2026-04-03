#!/bin/bash
echo "=== Incremental Loading Simulation ==="
echo "Adding 10 new records every 15 minutes for 4 rounds"
echo "Started at: $(date)"
echo ""

for i in 1 2 3 4; do
    echo "--- Round $i of 4 --- $(date)"

    # Always work from the latest version in S3
    aws s3 cp s3://truist-mini-bronze-jfd/source/core_banking.db /tmp/core_banking.db

    # Add new records with current timestamp (always newer than watermark)
    python3 - << 'PYEOF'
import sqlite3, random
from datetime import datetime, timezone, timedelta
from faker import Faker
fake = Faker()

conn = sqlite3.connect('/tmp/core_banking.db')
cursor = conn.cursor()

LOAN_TYPES = ['MORTGAGE', 'AUTO', 'PERSONAL', 'HELOC', 'STUDENT']
STATUSES   = ['CURRENT', 'DELINQUENT', 'DEFAULT', 'PAID_OFF', 'CHARGED_OFF']

# Get current max loan ID to avoid conflicts
cursor.execute("SELECT MAX(CAST(SUBSTR(loan_id, 2) AS INTEGER)) FROM loan_master")
max_id = cursor.fetchone()[0] or 500

now = datetime.now(timezone.utc)
rows = []
for i in range(10):
    modified = now + timedelta(seconds=i)
    new_id = max_id + i + 1
    rows.append((
        f"L{str(new_id).zfill(7)}",
        f"C{fake.numerify(text='#####')}",
        random.choice(LOAN_TYPES),
        fake.date_between(start_date='-5y', end_date='-1y').isoformat(),
        round(random.uniform(5000, 500000), 2),
        round(random.uniform(2.5, 8.5), 3),
        random.choice([60, 120, 180, 240, 360]),
        random.choice(STATUSES),
        f"BR{fake.numerify(text='###')}",
        modified.strftime('%Y-%m-%d %H:%M:%S')
    ))

cursor.executemany('INSERT OR REPLACE INTO loan_master VALUES (?,?,?,?,?,?,?,?,?,?)', rows)
conn.commit()
cursor.execute("SELECT COUNT(*) FROM loan_master")
total = cursor.fetchone()[0]
print(f"Inserted 10 new rows. Total records: {total}")
conn.close()
PYEOF

    # Push updated database back to S3
    aws s3 cp /tmp/core_banking.db s3://truist-mini-bronze-jfd/source/core_banking.db
    echo "Uploaded updated database to S3"

    # Show current counts
    echo "Bronze streaming files: $(aws s3 ls s3://truist-mini-bronze-jfd/topics/loan-events-raw/ --recursive | wc -l)"
    echo "Bronze batch files: $(aws s3 ls s3://truist-mini-bronze-jfd/batch/core_banking/loan_master/ --recursive | wc -l)"
    echo ""

    if [ $i -lt 4 ]; then
        echo "Waiting 15 minutes..."
        sleep 900
    fi
done

echo "=== Simulation complete at $(date) ==="
