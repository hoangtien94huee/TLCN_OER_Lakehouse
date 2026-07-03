FROM apache/airflow:2.9.2-python3.12

USER root

# Install all system deps in a single layer: Java (for PySpark), Selenium/Chrome deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    unzip \
    curl \
    xvfb \
    postgresql-client \
    libpq-dev \
    openjdk-17-jdk-headless \
    fonts-liberation \
    libasound2 libatk-bridge2.0-0 libatk1.0-0 libatspi2.0-0 libcups2 \
    libdbus-1-3 libdrm2 libgbm1 libgtk-3-0 libnspr4 libnss3 libwayland-client0 \
    libxcomposite1 libxdamage1 libxfixes3 libxkbcommon0 libxrandr2 xdg-utils \
    && wget -q "https://storage.googleapis.com/chrome-for-testing-public/140.0.7339.82/linux64/chrome-linux64.zip" -O /tmp/chrome.zip \
    && unzip -q /tmp/chrome.zip -d /opt/ \
    && ln -s /opt/chrome-linux64/chrome /usr/bin/google-chrome \
    && ln -s /opt/chrome-linux64/chrome /usr/bin/google-chrome-stable \
    && wget -q "https://storage.googleapis.com/chrome-for-testing-public/140.0.7339.82/linux64/chromedriver-linux64.zip" -O /tmp/chromedriver.zip \
    && unzip -q /tmp/chromedriver.zip -d /tmp/ \
    && mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver \
    && chmod +x /usr/local/bin/chromedriver \
    && rm -rf /tmp/*.zip /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

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

# Install Python dependencies with upgrade flag to ensure all dependencies are resolved
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -U -r /opt/airflow/requirements.txt

# Copy source code and dags
COPY src/ /opt/airflow/src/
COPY dags/ /opt/airflow/dags/
# Copy init scripts (MinIO)
RUN mkdir -p /opt/airflow/scripts
COPY src/create_schema.py /opt/airflow/scripts/create_schema.py

# Copy entrypoint script
COPY entrypoint.sh /opt/airflow/entrypoint.sh

# Switch to root to set permissions and fix line endings
USER root

# Fix line endings and set execute permissions for entrypoint
RUN sed -i 's/\r$//' /opt/airflow/entrypoint.sh && chmod +x /opt/airflow/entrypoint.sh

# Set environment variables for production
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH="/opt/airflow:/opt/airflow/dags"
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
# Ensure Java and ChromeDriver are in PATH
ENV PATH="${JAVA_HOME}/bin:/usr/local/bin:${PATH}"

# Switch back to airflow user
USER airflow

# Expose port
EXPOSE 8080

ENTRYPOINT ["/opt/airflow/entrypoint.sh"]
