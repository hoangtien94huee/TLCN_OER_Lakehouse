#!/bin/bash
# OER Lakehouse - Simple Start Script
# ===================================

set -e

echo "🚀 Starting OER Lakehouse (Simple Mode)..."

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if Docker and Docker Compose are installed
echo -e "${BLUE}🔍 Checking requirements...${NC}"
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker is not installed${NC}"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}❌ Docker Compose is not installed${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Docker and Docker Compose are available${NC}"

# Create necessary directories
echo -e "${BLUE}📁 Creating directories...${NC}"
mkdir -p lakehouse/data/{scraped,logs,spark,notebooks,iceberg-jars}

# Start deployment
echo -e "${BLUE}🚀 Starting OER Lakehouse services...${NC}"

# Step 1: Core Infrastructure
echo -e "${YELLOW}1️⃣  Starting databases...${NC}"
docker-compose up -d postgres metastore-db minio
sleep 10

# Step 2: Data Processing
echo -e "${YELLOW}2️⃣  Starting data processing...${NC}"
docker-compose up -d hive-metastore spark-master spark-worker
sleep 15

# Step 3: Application Layer
echo -e "${YELLOW}3️⃣  Starting applications...${NC}"
docker-compose up -d airflow jupyter
sleep 10

# Health checks
echo -e "${BLUE}🏥 Checking services...${NC}"

services=("postgres:5432" "minio:9000" "airflow:8080" "jupyter:8888")
for service in "${services[@]}"; do
    IFS=':' read -ra ADDR <<< "$service"
    service_name=${ADDR[0]}
    
    echo -e "${YELLOW}Checking ${service_name}...${NC}"
    if docker-compose ps | grep -q "${service_name}.*Up"; then
        echo -e "${GREEN}✅ ${service_name} is running${NC}"
    else
        echo -e "${RED}❌ ${service_name} is not running${NC}"
    fi
done

# Display service URLs
echo -e "\n${GREEN}🎉 OER Lakehouse is ready!${NC}"
echo -e "\n${BLUE}📊 Available Services:${NC}"
echo -e "┌─────────────────────────────────────────────┐"
echo -e "│ Service        │ URL                       │"
echo -e "├─────────────────────────────────────────────┤"
echo -e "│ Airflow        │ http://localhost:8080     │"
echo -e "│ Jupyter        │ http://localhost:8888     │"
echo -e "│ MinIO Console  │ http://localhost:9001     │"
echo -e "│ Spark Master   │ http://localhost:8081     │"
echo -e "└─────────────────────────────────────────────┘"

echo -e "\n${BLUE}📚 Quick Start:${NC}"
echo -e "1. Open Airflow: http://localhost:8080 (admin/admin)"
echo -e "2. Open Jupyter: http://localhost:8888"
echo -e "3. Run scripts: docker exec oer-airflow python /opt/airflow/scripts/create_schema.py"

echo -e "\n${GREEN}✨ Simple lakehouse ready to use!${NC}"
