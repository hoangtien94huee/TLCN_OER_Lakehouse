# Dockerfile for Airflow OER Scraper
FROM apache/airflow:2.7.1-python3.10

# Switch to root user to install system dependencies
USER root

# Install system dependencies for Chrome, Selenium, PostgreSQL, and Java (for Spark)
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    xvfb \
    postgresql-client \
    openjdk-11-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install Apache Spark 3.5.0
RUN wget -q https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz \
    && tar -xzf spark-3.5.0-bin-hadoop3.tgz -C /opt/ \
    && mv /opt/spark-3.5.0-bin-hadoop3 /opt/spark \
    && rm spark-3.5.0-bin-hadoop3.tgz \
    && chown -R airflow:root /opt/spark

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=/usr/local/bin/python

# Install Google Chrome (latest stable)
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Install ChromeDriver compatible with latest Chrome
RUN apt-get update && \
    apt-get remove -y chromium-driver chromedriver || true && \
    rm -f /usr/bin/chromedriver /usr/local/bin/chromedriver || true && \
    wget -O /tmp/chromedriver.zip "https://storage.googleapis.com/chrome-for-testing-public/140.0.7339.82/linux64/chromedriver-linux64.zip" && \
    unzip /tmp/chromedriver.zip -d /tmp/ && \
    mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver && \
    rm -rf /tmp/chromedriver.zip && \
    /usr/local/bin/chromedriver --version && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Create directories for data persistence
RUN mkdir -p /opt/airflow/data \
    && mkdir -p /opt/airflow/logs \
    && mkdir -p /opt/airflow/scraped_data \
    && mkdir -p /opt/airflow/database \
    && mkdir -p /opt/airflow/spark_data \
    && mkdir -p /opt/airflow/spark_jars

# Download Iceberg JARs for Spark integration
RUN wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
    -O /opt/airflow/spark_jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
    && wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.565/aws-java-sdk-bundle-1.12.565.jar \
    -O /opt/airflow/spark_jars/aws-java-sdk-bundle-1.12.565.jar \
    && wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -O /opt/airflow/spark_jars/hadoop-aws-3.3.4.jar

# Set permissions
RUN chown -R airflow:root /opt/airflow/data \
    && chown -R airflow:root /opt/airflow/scraped_data \
    && chown -R airflow:root /opt/airflow/database \
    && chown -R airflow:root /opt/airflow/spark_data \
    && chown -R airflow:root /opt/airflow/spark_jars

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /opt/airflow/

# Install Python dependencies
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copy source code and dags
COPY src/ /opt/airflow/src/
COPY dags/ /opt/airflow/dags/

# Copy entrypoint script
COPY entrypoint.sh /opt/airflow/entrypoint.sh

# Switch to root to set permissions and fix line endings
USER root

# Fix line endings and set execute permissions for entrypoint
RUN sed -i 's/\r$//' /opt/airflow/entrypoint.sh && \
    chmod +x /opt/airflow/entrypoint.sh

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
