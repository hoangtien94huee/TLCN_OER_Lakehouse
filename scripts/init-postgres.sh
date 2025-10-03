#!/bin/bash
set -e

# Tạo database cho Iceberg REST Catalog
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE iceberg;
    CREATE USER iceberg WITH PASSWORD 'iceberg';
    GRANT ALL PRIVILEGES ON DATABASE iceberg TO iceberg;
    
    \c iceberg
    GRANT ALL ON SCHEMA public TO iceberg;
    ALTER DATABASE iceberg OWNER TO iceberg;
EOSQL

echo "Iceberg catalog database created successfully"
