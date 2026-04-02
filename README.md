# Truist Mini Platform

A free local replica of the Truist Financial cloud-native data platform, built as a portfolio project to demonstrate enterprise-grade data engineering patterns using open-source tools and AWS free tier services.

## Architecture Overview

This project mirrors the production Truist AWS data platform with the following substitutions:

| Production (Truist AWS) | Local Substitute | Why |
|---|---|---|
| Amazon EKS | kind (Kubernetes in Docker) | Same kubectl commands, same YAML specs |
| Amazon MSK (Kafka 3.7) | Apache Kafka 3.7.1 in Docker (KRaft) | Identical Kafka API |
| EKS Producer/Consumer Pods | Python containers on kind | Same Deployment specs, same IRSA pattern |
| AWS Glue PySpark | Real AWS Glue (free tier) | No substitution needed |
| Amazon S3 | Real AWS S3 (free tier) | No substitution needed |
| DynamoDB Watermarks | Real AWS DynamoDB (free tier) | No substitution needed |
| Athena / Redshift Spectrum | Real AWS Athena (free tier) | No substitution needed |
| MWAA (Airflow) | Local Airflow in Docker Compose | Same DAG syntax |
| Oracle over Direct Connect | SQLite (downloaded from S3 at runtime) | Same watermark logic |
| IRSA (per-pod IAM) | Kubernetes Secret | Same concept, simplified mechanism |

## Data Flow
```
                    STREAMING PATH
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│  Python Faker   │───▶│  Kafka 3.7.1 │───▶│  Consumer Pod   │
│  Producer Pod   │    │  (KRaft)     │    │  (kind cluster) │
│  (kind cluster) │    │  3 topics    │    │  manual commits │
└─────────────────┘    └──────────────┘    └────────┬────────┘
                                                    │
                                                    ▼
                    BATCH PATH              S3 Bronze
┌─────────────────┐    ┌──────────────┐    (NDJSON + Parquet)
│  SQLite Source  │───▶│  AWS Glue    │───▶        │
│  (core banking  │    │  Python Shell│            │
│   simulation)   │    │  + DynamoDB  │            │
└─────────────────┘    │  Watermark   │            │
                       └──────────────┘            │
                                                   ▼
                                          AWS Glue PySpark
                                          Silver Layer
                                          (dedup, types, SCD2)
                                                   │
                                                   ▼
                                          AWS Glue PySpark
                                          Gold Layer
                                          (star schema, CCAR)
                                                   │
                                                   ▼
                                          Amazon Athena
                                          (SQL query layer)
```

## Medallion Architecture

### Bronze — Raw Landing Zone
- **Streaming**: `s3://truist-mini-bronze-jfd/topics/loan-events-raw/year=/month=/day=/hour=/` — NDJSON, appended by consumer pods, partitioned by event timestamp
- **Batch**: `s3://truist-mini-bronze-jfd/batch/core_banking/loan_master/year=/month=/day=/` — Parquet, written by Glue JDBC job with DynamoDB watermark
- Append-only. Never modified after write. 7-year retention in production (SOX/CCAR).

### Silver — Transformed & Trusted
- `loan_master/` — partitioned by `loan_type`. Type normalization, deduplication by `row_number() OVER (PARTITION BY loan_id ORDER BY MODIFIED_TS DESC)`
- `loan_events/` — partitioned by `event_type`. Type normalization, deduplication by event identity (`loan_id + event_ts + event_type`)
- Snappy Parquet compression. Overwrite mode — always reflects current clean state.

### Gold — Dimensional Model
| Table | Type | Partition | Description |
|---|---|---|---|
| `fact_loan_performance` | Fact | `occ_classification` | Credit risk metrics with OCC classification and risk-weighted exposure |
| `fact_loan_event_activity` | Fact | `event_type` | Row-level events enriched with loan master data (streaming + batch join) |
| `fact_event_summary` | Aggregate | `event_type` | Pre-aggregated event metrics by type, branch, month |
| `dim_status` | Dimension | — | OCC risk classification with loss_given_default |
| `dim_loan_type` | Dimension | — | Loan category and collateral type |

## Key Engineering Concepts Demonstrated

**At-least-once delivery with manual offset commits** — Consumer pods commit Kafka offsets only after confirming S3 write success. Guarantees no data loss on pod crash. Duplicates handled by silver deduplication.

**DynamoDB watermark pattern** — Glue batch jobs read last successful extraction timestamp from DynamoDB, extract only new rows, update watermark atomically on success. Transparent and manually resettable — superior to Glue bookmarks.

**Incremental extraction** — `WHERE MODIFIED_TS > last_watermark` ensures only changed records are extracted on each run. Proven by adding new SQLite rows and confirming only those rows are extracted.

**Star schema with CCAR-relevant metrics** — `fact_loan_performance` includes OCC loan classification (Pass/Special Mention/Substandard/Loss), `loss_given_default`, and `risk_weighted_exposure = estimated_balance * loss_given_default`. Direct input to credit risk stress testing models.

**Dual-listener Kafka configuration** — Mirrors eps-pipeline pattern. All listeners bind to `0.0.0.0` to avoid IP bind errors at container startup. Advertised listeners use hostname (`truist-kafka`) for Docker DNS resolution — no hardcoded IPs, no session-to-session drift.

**Rolling updates with zero downtime** — Kubernetes Deployment rolling update strategy with `maxUnavailable=0`. Old pod terminates only after new pod is running and ready.

**Docker layer caching** — Dockerfile orders dependencies before application code. `pip install` layer cached — only the `COPY` layer rebuilds on code changes. Fast iteration.

## Project Structure
```
truist-mini/
├── kafka/
│   └── docker-compose.yaml          # Kafka 3.7.1 KRaft, dual-listener config
├── k8s/
│   ├── kind-cluster.yaml            # 3-node kind cluster config
│   ├── loan-producer-deployment.yaml
│   └── loan-consumer-deployment.yaml
├── producers/
│   ├── loan_producer.py             # Faker-based loan event generator
│   └── Dockerfile
├── consumers/
│   ├── loan_consumer.py             # Kafka → S3 bronze with manual offsets
│   └── Dockerfile
├── glue/
│   ├── create_source_db.py          # Creates SQLite core banking simulation
│   ├── add_new_records.py           # Adds new rows to test incremental extraction
│   ├── loan_master_batch.py         # Glue Python Shell: incremental batch extraction
│   ├── loan_master_silver.py        # Glue PySpark: bronze → silver (batch)
│   ├── loan_events_silver.py        # Glue PySpark: bronze → silver (streaming)
│   ├── loan_master_gold.py          # Glue PySpark: silver → gold (credit risk)
│   └── loan_events_gold.py          # Glue PySpark: silver → gold (event activity)
├── athena/
│   └── create_tables.sql            # DDL for all 5 gold tables in Glue Catalog
├── terraform/
│   ├── main.tf                      # Provider config
│   ├── variables.tf                 # Input variables
│   ├── s3.tf                        # Bronze, silver, gold, athena buckets
│   ├── dynamodb.tf                  # pipeline-watermarks table
│   ├── iam.tf                       # Glue service role + policy attachments
│   ├── glue.tf                      # 5 Glue jobs + Glue Catalog database
│   ├── athena.tf                    # Athena workgroup
│   └── outputs.tf                   # Resource name outputs
├── start.sh                         # One-command session startup
└── README.md
```

## Prerequisites

- Docker Desktop with WSL2 backend
- kubectl v1.35+
- kind v0.27+
- Python 3.12+
- AWS CLI v2 configured (`aws configure`, region `us-east-1`)
- AWS account with permissions for S3, Glue, DynamoDB, Athena, IAM

## Quick Start

### First-time setup
```bash
# Install kind
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.27.0/kind-linux-amd64
chmod +x ./kind && sudo mv ./kind /usr/local/bin/kind

# Create S3 buckets (replace jfd with your initials)
aws s3api create-bucket --bucket truist-mini-bronze-jfd --region us-east-1
aws s3api create-bucket --bucket truist-mini-silver-jfd --region us-east-1
aws s3api create-bucket --bucket truist-mini-gold-jfd --region us-east-1
aws s3api create-bucket --bucket truist-mini-athena-results-jfd --region us-east-1

# Create DynamoDB watermark table
aws dynamodb create-table \
  --table-name pipeline-watermarks \
  --attribute-definitions AttributeName=source_table,AttributeType=S \
  --key-schema AttributeName=source_table,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1

# Create SQLite source database and upload to S3
python3 glue/create_source_db.py
aws s3 cp glue/core_banking.db s3://truist-mini-bronze-jfd/source/core_banking.db

# Upload Glue scripts
aws s3 cp glue/loan_master_batch.py s3://truist-mini-bronze-jfd/glue-scripts/
aws s3 cp glue/loan_master_silver.py s3://truist-mini-bronze-jfd/glue-scripts/
aws s3 cp glue/loan_events_silver.py s3://truist-mini-bronze-jfd/glue-scripts/
aws s3 cp glue/loan_master_gold.py s3://truist-mini-bronze-jfd/glue-scripts/
aws s3 cp glue/loan_events_gold.py s3://truist-mini-bronze-jfd/glue-scripts/

# Create IAM role for Glue
aws iam create-role --role-name truist-mini-glue-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"glue.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
aws iam attach-role-policy --role-name truist-mini-glue-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
aws iam attach-role-policy --role-name truist-mini-glue-role \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
aws iam attach-role-policy --role-name truist-mini-glue-role \
  --policy-arn arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess

# Build Docker images
docker build -t loan-producer:v2 producers/
docker build -t loan-consumer:v1 consumers/
```

### Every session
```bash
~/truist-mini/start.sh
```

### Shutdown
```bash
cd kafka && docker compose down
kind delete cluster --name truist-mini
```

## Running the Batch Pipeline
```bash
# Run batch extraction (bronze)
aws glue start-job-run --job-name truist-mini-loan-master-batch --region us-east-1

# Run silver transformation
aws glue start-job-run --job-name truist-mini-silver-loan-master --region us-east-1

# Run gold transformation
aws glue start-job-run --job-name truist-mini-gold-loan-master --region us-east-1
aws glue start-job-run --job-name truist-mini-gold-loan-events --region us-east-1
```

## Sample Athena Queries
```sql
-- Risk exposure by OCC classification (CCAR input)
SELECT occ_classification,
       COUNT(*) as loan_count,
       CAST(SUM(risk_weighted_exposure) AS DECIMAL(18,2)) as total_risk_exposure
FROM truist_mini.fact_loan_performance
GROUP BY occ_classification
ORDER BY total_risk_exposure DESC;

-- Event volume by type (streaming datamart)
SELECT event_type,
       COUNT(*) as event_count,
       CAST(SUM(amount) AS DECIMAL(18,2)) as total_volume
FROM truist_mini.fact_loan_event_activity
GROUP BY event_type
ORDER BY event_count DESC;

-- Delinquency rate by loan type
SELECT loan_type,
       COUNT(*) as total_loans,
       SUM(CASE WHEN is_delinquent THEN 1 ELSE 0 END) as delinquent_loans,
       CAST(100.0 * SUM(CASE WHEN is_delinquent THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) as delinquency_rate
FROM truist_mini.fact_loan_performance
GROUP BY loan_type
ORDER BY delinquency_rate DESC;
```

## Production Mapping Notes

This project intentionally mirrors Truist production patterns:
- DynamoDB watermark pattern used in production `pipeline-watermarks` table
- Manual Kafka offset commits matching production consumer pod behavior
- Medallion architecture (bronze/silver/gold) matching production S3 bucket structure
- OCC loan classification matching regulatory reporting requirements
- Star schema dimensional model matching production Redshift gold layer
- Glue PySpark deduplication using `row_number()` window function matching production silver jobs

## Author

Joe Davis — Lead Data Engineer  
[GitHub](https://github.com/joefdavis4444) | [LinkedIn](https://linkedin.com/in/joefdavis)
