@echo off
echo Building and starting OER Scraper with PostgreSQL...

echo.
echo Stopping any existing containers...
docker-compose down

echo.
echo Building new images...
docker-compose build --no-cache

echo.
echo Starting services...
docker-compose up -d

echo.
echo Waiting for services to start...
timeout /t 40 /nobreak

echo.
echo Checking service status...
docker-compose ps

echo.
echo Checking Airflow logs...
docker-compose logs oer-scraper --tail=10

echo.
echo ============================================
echo ✅ Services started successfully!
echo ============================================
echo.
echo 🌐 Airflow WebUI: http://localhost:8080
echo    Username: admin
echo    Password: admin
echo.
echo 🗄️ PostgreSQL: localhost:5432
echo    Database: oer_scraper (application data)
echo    Database: airflow (Airflow metadata)  
echo    Username: airflow  
echo    Password: airflow
echo.
echo 📊 To view logs:
echo    docker-compose logs oer-scraper
echo    docker-compose logs postgres
echo.
echo 🛑 To stop services:
echo    docker-compose down
echo.
pause
