#!/bin/bash

set -e

echo "⏳ Waiting for Postgres..."

# Wait until Postgres is ready
while ! nc -z postgres 5432; do
  sleep 2
done

echo "✅ Postgres is ready"

echo "🛠 Running Airflow migrations..."

# This is idempotent → safe to run always
airflow db upgrade

echo "👤 Creating admin user (if not exists)..."

airflow users create \
    --username admin \
    --password admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com || true

echo "🚀 Starting Airflow: $@"

exec airflow "$@"