import sqlite3
import random
from datetime import datetime, timezone, timedelta
from faker import Faker

fake = Faker()

conn = sqlite3.connect('/home/joseph_davis/truist-mini/glue/core_banking.db')
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS loan_master (
        loan_id         TEXT PRIMARY KEY,
        customer_id     TEXT NOT NULL,
        loan_type       TEXT NOT NULL,
        origination_dt  TEXT NOT NULL,
        principal       REAL NOT NULL,
        interest_rate   REAL NOT NULL,
        term_months     INTEGER NOT NULL,
        status          TEXT NOT NULL,
        branch_id       TEXT NOT NULL,
        MODIFIED_TS     TEXT NOT NULL
    )
''')

LOAN_TYPES   = ['MORTGAGE', 'AUTO', 'PERSONAL', 'HELOC', 'STUDENT']
STATUSES     = ['CURRENT', 'DELINQUENT', 'DEFAULT', 'PAID_OFF', 'CHARGED_OFF']

base_time = datetime.now(timezone.utc) - timedelta(days=30)

rows = []
for i in range(500):
    modified = base_time + timedelta(minutes=i * 5)
    rows.append((
        f"L{str(i+1).zfill(7)}",
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

cursor.executemany('''
    INSERT OR REPLACE INTO loan_master VALUES (?,?,?,?,?,?,?,?,?,?)
''', rows)

conn.commit()
print(f"Created core_banking.db with {len(rows)} loan records")
print(f"Date range: {rows[0][9]} to {rows[-1][9]}")

cursor.execute("SELECT COUNT(*) FROM loan_master")
print(f"Verified row count: {cursor.fetchone()[0]}")
conn.close()
