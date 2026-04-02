#!/bin/bash
echo "=== Starting Truist Mini Airflow ==="
export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)
cd ~/truist-mini/airflow && docker compose up -d
sleep 30
docker compose exec airflow-scheduler airflow dags list
echo ""
echo "=== Airflow UI: http://localhost:8080 ==="
echo "=== Username: admin | Password: admin ==="
