@echo off
echo Cleaning old OER Scraper data and containers...

echo.
echo 1. Stopping Docker containers...
docker-compose down

echo.
echo 2. Cleaning Docker system...
docker system prune -f
docker volume prune -f

echo.
echo 3. Removing old data directories...
if exist "logs" rmdir /s /q logs
if exist "scraped_data" rmdir /s /q scraped_data
if exist "dags\__pycache__" rmdir /s /q dags\__pycache__

echo.
echo 4. Removing old database files...
if exist "database\airflow.db" del database\airflow.db
if exist "database\scraped_documents.db" del database\scraped_documents.db

echo.
echo 5. Recreating necessary directories...
mkdir logs 2>nul
mkdir scraped_data 2>nul
mkdir database 2>nul

echo.
echo ✅ Cleanup completed! Ready for fresh deployment.
echo.
echo To deploy fresh:
echo   deploy-postgres.bat
echo.
pause
