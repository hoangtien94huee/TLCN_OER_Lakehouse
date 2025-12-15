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

# Tạo database cho DSpace
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER dspace WITH PASSWORD 'dspace';
    CREATE DATABASE dspace OWNER dspace;
    GRANT ALL PRIVILEGES ON DATABASE dspace TO dspace;
    
    \c dspace
    CREATE EXTENSION IF NOT EXISTS pgcrypto;
EOSQL

echo "DSpace database created successfully"
