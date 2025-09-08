-- Tạo database cho OER scraper application
CREATE DATABASE oer_scraper;

-- Kết nối đến database mới tạo
\c oer_scraper;

-- Tạo bảng scraped_documents với PostgreSQL syntax
CREATE TABLE IF NOT EXISTS scraped_documents (
    id VARCHAR(255) PRIMARY KEY,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    content_hash VARCHAR(32) NOT NULL,
    source VARCHAR(100) NOT NULL,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tạo bảng scraping_logs
CREATE TABLE IF NOT EXISTS scraping_logs (
    id SERIAL PRIMARY KEY,
    run_date DATE NOT NULL,
    total_found INTEGER DEFAULT 0,
    new_documents INTEGER DEFAULT 0,
    updated_documents INTEGER DEFAULT 0,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tạo indexes để tối ưu performance
CREATE INDEX IF NOT EXISTS idx_scraped_documents_url ON scraped_documents(url);
CREATE INDEX IF NOT EXISTS idx_scraped_documents_source ON scraped_documents(source);
CREATE INDEX IF NOT EXISTS idx_scraped_documents_last_updated ON scraped_documents(last_updated);
CREATE INDEX IF NOT EXISTS idx_scraping_logs_run_date ON scraping_logs(run_date);
CREATE INDEX IF NOT EXISTS idx_scraping_logs_status ON scraping_logs(status);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;
