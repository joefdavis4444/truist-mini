import sqlite3
import random
from datetime import datetime, timezone, timedelta
from faker import Faker

fake = Faker()

conn = sqlite3.connect('/tmp/core_banking.db')
cursor = conn.cursor()

LOAN_TYPES = ['MORTGAGE', 'AUTO', 'PERSONAL', 'HELOC', 'STUDENT']
STATUSES   = ['CURRENT', 'DELINQUENT', 'DEFAULT', 'PAID_OFF', 'CHARGED_OFF']

new_time = datetime.now(timezone.utc)
rows = []
for i in range(10):
    modified = new_time + timedelta(minutes=i)
    rows.append((
        f"L{str(600+i).zfill(7)}",
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
print(f"Inserted {len(rows)} new rows with MODIFIED_TS around {new_time.strftime('%Y-%m-%d %H:%M:%S')}")
conn.close()
