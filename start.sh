#!/bin/bash
set -e

echo "=== Starting Truist Mini Platform ==="

# 1. Start Kafka
echo "[1/7] Starting Kafka..."
cd ~/truist-mini/kafka && docker compose up -d
sleep 8

# 2. Recreate topics
echo "[2/7] Creating topics..."
docker exec truist-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic loan-events-raw --partitions 3 --replication-factor 1 2>/dev/null || echo "  loan-events-raw already exists"
docker exec truist-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic app-events-raw --partitions 3 --replication-factor 1 2>/dev/null || echo "  app-events-raw already exists"
docker exec truist-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic credit-scores-raw --partitions 3 --replication-factor 1 2>/dev/null || echo "  credit-scores-raw already exists"

# 3. Create kind cluster
echo "[3/7] Creating kind cluster..."
kind create cluster --config ~/truist-mini/k8s/kind-cluster.yaml 2>/dev/null || echo "  Cluster already exists"

# 4. Setup namespace
echo "[4/7] Setting up namespace..."
kubectl create namespace data-platform 2>/dev/null || echo "  Namespace already exists"
kubectl config set-context --current --namespace=data-platform

# 5. Connect Kafka to kind network
echo "[5/7] Connecting Kafka to kind network..."
docker network connect kind truist-kafka 2>/dev/null || echo "  Already connected"
echo "  Kafka hostname: truist-kafka"

# 6. Create AWS secret and deploy pods
echo "[6/7] Deploying pods..."
kubectl create secret generic aws-credentials \
  --from-literal=aws_access_key_id=$(aws configure get aws_access_key_id) \
  --from-literal=aws_secret_access_key=$(aws configure get aws_secret_access_key) \
  --namespace=data-platform 2>/dev/null || echo "  Secret already exists"

kind load docker-image loan-producer:v2 --name truist-mini
kind load docker-image loan-consumer:v1 --name truist-mini

kubectl apply -f ~/truist-mini/k8s/loan-producer-deployment.yaml
kubectl apply -f ~/truist-mini/k8s/loan-consumer-deployment.yaml

echo ""
echo "=== Startup Complete ==="
kubectl get pods
