#!/bin/bash
# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL..."
while ! pg_isready -h postgres -p 5432 -U airflow; do
    echo "PostgreSQL is unavailable - sleeping"
    sleep 1
done
echo "PostgreSQL is up - continuing"

# Initialize MinIO buckets and lakehouse structure
echo "Initializing MinIO lakehouse structure..."
python3 /opt/airflow/scripts/create_schema.py || echo "MinIO init completed"

# Initialize Airflow database
echo "Initializing Airflow database..."
airflow db init

# Create admin user
echo "Creating Airflow admin user..."
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com || echo "Admin user already exists"

# Start Airflow scheduler in background
echo "Starting Airflow scheduler..."
airflow scheduler &

# Start Airflow webserver
echo "Starting Airflow webserver..."
exec airflow webserver --port 8080
