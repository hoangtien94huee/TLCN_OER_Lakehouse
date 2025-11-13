ARG ICEBERG_VERSION=1.4.3
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.565
# Optimized Dockerfile for Airflow OER Scraper - Processing moved to Spark cluster
FROM apache/airflow:2.9.2-python3.12

# Switch to root user to install system dependencies
USER root

# Install system dependencies for Chrome, Selenium, PostgreSQL, and Java for PySpark
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    xvfb \
    postgresql-client \
    libpq-dev \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Spark client connectivity
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64



RUN apt-get update && apt-get install -y wget unzip fonts-liberation libappindicator3-1 \
    libasound2 libatk-bridge2.0-0 libatk1.0-0 libatspi2.0-0 libcups2 \
    libdbus-1-3 libdrm2 libgbm1 libgtk-3-0 libnspr4 libnss3 libwayland-client0 \
    libxcomposite1 libxdamage1 libxfixes3 libxkbcommon0 libxrandr2 xdg-utils \
    # Download Chrome 140 for Testing
    && wget -q "https://storage.googleapis.com/chrome-for-testing-public/140.0.7339.82/linux64/chrome-linux64.zip" -O /tmp/chrome.zip \
    && unzip -q /tmp/chrome.zip -d /opt/ \
    && ln -s /opt/chrome-linux64/chrome /usr/bin/google-chrome \
    && ln -s /opt/chrome-linux64/chrome /usr/bin/google-chrome-stable \
    # Download ChromeDriver 140
    && wget -q "https://storage.googleapis.com/chrome-for-testing-public/140.0.7339.82/linux64/chromedriver-linux64.zip" -O /tmp/chromedriver.zip \
    && unzip -q /tmp/chromedriver.zip -d /tmp/ \
    && mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver \
    && chmod +x /usr/local/bin/chromedriver \
    && rm -rf /tmp/*.zip \
    # Verify versions
    && echo "=== Installed versions ===" \
    && google-chrome --version \
    && chromedriver --version \
    && rm -rf /var/lib/apt/lists/*
# # Install Google Chrome (latest stable)
# RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -     && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list     && apt-get update     && apt-get install -y google-chrome-stable     && rm -rf /var/lib/apt/lists/*

# # Install ChromeDriver compatible with latest Chrome
# RUN apt-get update &&     apt-get remove -y chromium-driver chromedriver || true &&     rm -f /usr/bin/chromedriver /usr/local/bin/chromedriver || true &&     wget -O /tmp/chromedriver.zip "https://storage.googleapis.com/chrome-for-testing-public/140.0.7339.82/linux64/chromedriver-linux64.zip" &&     unzip /tmp/chromedriver.zip -d /tmp/ &&     mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver &&     chmod +x /usr/local/bin/chromedriver &&     rm -rf /tmp/chromedriver.zip &&     /usr/local/bin/chromedriver --version &&     apt-get clean && rm -rf /var/lib/apt/lists/*

# Create directories for data persistence - simplified
RUN mkdir -p /opt/airflow/data \
    && mkdir -p /opt/airflow/scraped_data \
    && mkdir -p /opt/airflow/database

# Set permissions
RUN chown -R airflow:root /opt/airflow/data \
    && chown -R airflow:root /opt/airflow/scraped_data \
    && chown -R airflow:root /opt/airflow/database

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /opt/airflow/

# Install Python dependencies (force no hash check to avoid mismatch)
RUN pip install --no-cache-dir --no-deps -r /opt/airflow/requirements.txt ||     pip install --no-cache-dir --force-reinstall -r /opt/airflow/requirements.txt

# Copy source code and dags
COPY src/ /opt/airflow/src/
COPY dags/ /opt/airflow/dags/

# Copy entrypoint script
COPY entrypoint.sh /opt/airflow/entrypoint.sh

# Switch to root to set permissions and fix line endings
USER root

# Fix line endings and set execute permissions for entrypoint
RUN sed -i 's/\r$//' /opt/airflow/entrypoint.sh && chmod +x /opt/airflow/entrypoint.sh

# Set environment variables for production
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow:/opt/airflow/dags"
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
# Ensure ChromeDriver 140 is found first in PATH
ENV PATH="/usr/local/bin:${PATH}"

# Switch back to airflow user
USER airflow

# Expose port
EXPOSE 8080

ENTRYPOINT ["/opt/airflow/entrypoint.sh"]
