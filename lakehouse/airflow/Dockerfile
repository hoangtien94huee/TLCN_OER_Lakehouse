ARG ICEBERG_VERSION=1.4.3
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.565
# Dockerfile for Airflow OER Scraper
FROM bitnamilegacy/spark:3.5.4 AS spark-base

ARG ICEBERG_VERSION=1.4.3
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.565

USER root
RUN apt-get update && apt-get install -y curl wget && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/bitnami/spark/jars &&     curl -fsSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar -o /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar &&     curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -o /opt/bitnami/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar &&     curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar

USER 1001

FROM apache/airflow:2.7.1-python3.10

ARG ICEBERG_VERSION=1.4.3
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.565

# Switch to root user to install system dependencies
USER root

# Install system dependencies for Chrome, Selenium, PostgreSQL, Java (for Spark), and SASL
RUN apt-get update && apt-get install -y     wget     gnupg     unzip     curl     xvfb     postgresql-client     openjdk-11-jdk     libsasl2-dev     libsasl2-modules     libsasl2-modules-gssapi-mit     build-essential     python3-dev     && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Spark client connectivity
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install Google Chrome (latest stable)
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -     && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list     && apt-get update     && apt-get install -y google-chrome-stable     && rm -rf /var/lib/apt/lists/*

# Install ChromeDriver compatible with latest Chrome
RUN apt-get update &&     apt-get remove -y chromium-driver chromedriver || true &&     rm -f /usr/bin/chromedriver /usr/local/bin/chromedriver || true &&     wget -O /tmp/chromedriver.zip "https://storage.googleapis.com/chrome-for-testing-public/140.0.7339.82/linux64/chromedriver-linux64.zip" &&     unzip /tmp/chromedriver.zip -d /tmp/ &&     mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver &&     chmod +x /usr/local/bin/chromedriver &&     rm -rf /tmp/chromedriver.zip &&     /usr/local/bin/chromedriver --version &&     apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy Spark distribution from bitnamilegacy base image
COPY --from=spark-base /opt/bitnami/spark /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV ICEBERG_VERSION=${ICEBERG_VERSION}
ENV HADOOP_AWS_VERSION=${HADOOP_AWS_VERSION}
ENV AWS_SDK_BUNDLE_VERSION=${AWS_SDK_BUNDLE_VERSION}
ENV PYSPARK_SUBMIT_ARGS="--conf spark.jars=/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar,/opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar,/opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar pyspark-shell"

# Create directories for data persistence
RUN mkdir -p /opt/airflow/data     && mkdir -p /opt/airflow/logs     && mkdir -p /opt/airflow/scraped_data     && mkdir -p /opt/airflow/database     && mkdir -p /opt/airflow/spark_data

# Set permissions
RUN chown -R airflow:root /opt/airflow/data     && chown -R airflow:root /opt/airflow/scraped_data     && chown -R airflow:root /opt/airflow/database     && chown -R airflow:root /opt/airflow/spark_data

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /opt/airflow/

# Install Python dependencies (force no hash check to avoid mismatch)
RUN pip install --no-cache-dir --no-deps -r /opt/airflow/requirements.txt ||     pip install --no-cache-dir --force-reinstall -r /opt/airflow/requirements.txt

# Copy source code, dags, and scripts
COPY src/ /opt/airflow/src/
COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/

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
