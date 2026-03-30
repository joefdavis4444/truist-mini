import sys
import sqlite3
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import os
from datetime import datetime, timezone

# ── Config ──────────────────────────────────────────────
SOURCE_TABLE    = 'loan_master'
DB_S3_KEY       = 'source/core_banking.db'
DB_LOCAL_PATH   = '/tmp/core_banking.db'
BRONZE_BUCKET   = 'truist-mini-bronze-jfd'
WATERMARK_TABLE = 'pipeline-watermarks'
REGION          = 'us-east-1'
DEFAULT_WATERMARK = '2000-01-01 00:00:00'

dynamodb = boto3.resource('dynamodb', region_name=REGION)
s3       = boto3.client('s3', region_name=REGION)
wm_table = dynamodb.Table(WATERMARK_TABLE)

# ── Step 0: Download SQLite DB from S3 ──────────────────
print(f"[0/4] Downloading source database from S3...")
s3.download_file(BRONZE_BUCKET, DB_S3_KEY, DB_LOCAL_PATH)
print(f"      Downloaded to {DB_LOCAL_PATH}")

# ── Step 1: Read watermark ───────────────────────────────
print(f"[1/4] Reading watermark for {SOURCE_TABLE}...")
response = wm_table.get_item(Key={'source_table': SOURCE_TABLE})
item = response.get('Item', {})
last_watermark = item.get('last_watermark', DEFAULT_WATERMARK)
print(f"      Last watermark: {last_watermark}")

# ── Step 2: Extract from SQLite ──────────────────────────
print(f"[2/4] Extracting rows where MODIFIED_TS > '{last_watermark}'...")
conn = sqlite3.connect(DB_LOCAL_PATH)
query = f"""
    SELECT *,
           '{SOURCE_TABLE}'   AS _source_table,
           '{REGION}'         AS _source_system,
           datetime('now')    AS _ingestion_ts,
           '{last_watermark}' AS _watermark_start
    FROM {SOURCE_TABLE}
    WHERE MODIFIED_TS > '{last_watermark}'
    ORDER BY MODIFIED_TS ASC
"""
df = pd.read_sql_query(query, conn)
conn.close()

if df.empty:
    print("      No new rows found. Pipeline is up to date.")
    sys.exit(0)

print(f"      Extracted {len(df)} rows")
new_watermark = df['MODIFIED_TS'].max()
print(f"      New watermark will be: {new_watermark}")

# ── Step 3: Write Parquet to S3 bronze ───────────────────
print(f"[3/4] Writing to S3 bronze...")
now = datetime.now(timezone.utc)
s3_key = (
    f"batch/core_banking/{SOURCE_TABLE}/"
    f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
    f"{SOURCE_TABLE}_{now.strftime('%Y%m%d_%H%M%S')}.parquet"
)

table = pa.Table.from_pandas(df)
buf = io.BytesIO()
pq.write_table(table, buf)
buf.seek(0)

s3.put_object(
    Bucket=BRONZE_BUCKET,
    Key=s3_key,
    Body=buf.getvalue(),
    ContentType='application/octet-stream'
)
print(f"      Written: s3://{BRONZE_BUCKET}/{s3_key}")
print(f"      Rows: {len(df)} | Size: {len(buf.getvalue())} bytes")

# ── Step 4: Update watermark ─────────────────────────────
print(f"[4/4] Updating watermark to {new_watermark}...")
wm_table.put_item(Item={
    'source_table':   SOURCE_TABLE,
    'last_watermark': new_watermark,
    'last_run_ts':    now.isoformat(),
    'last_row_count': len(df),
    'status':         'SUCCESS'
})
print(f"      Watermark updated successfully.")
print(f"\n=== Batch job complete: {len(df)} rows extracted ===")
