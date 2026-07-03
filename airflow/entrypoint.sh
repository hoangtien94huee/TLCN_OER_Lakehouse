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
if [ -f /opt/airflow/scripts/create_schema.py ]; then
    python3 /opt/airflow/scripts/create_schema.py || echo "MinIO init completed"
else
    echo "create_schema.py not found, skipping MinIO init"
fi

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

# Start PageIndex chatbot API after Airflow/Spark warm-up (early start can hang uvicorn).
ENABLE_CHATBOT_API=${ENABLE_CHATBOT_API:-1}
if [ "$ENABLE_CHATBOT_API" = "1" ]; then
    CHATBOT_API_HOST=${CHATBOT_API_HOST:-0.0.0.0}
    CHATBOT_API_PORT=${CHATBOT_API_PORT:-8088}
    (
        sleep 20
        echo "Starting PageIndex chatbot API on ${CHATBOT_API_HOST}:${CHATBOT_API_PORT}..."
        exec python3 -m uvicorn chatbot_api:app \
            --app-dir /opt/airflow/src \
            --host "${CHATBOT_API_HOST}" \
            --port "${CHATBOT_API_PORT}" \
            >> /opt/airflow/logs/chatbot-api.log 2>&1
    ) &
else
    echo "ENABLE_CHATBOT_API=0 -> skipping chatbot API startup"
fi

# Start Airflow webserver
echo "Starting Airflow webserver..."
exec airflow webserver --port 8080
