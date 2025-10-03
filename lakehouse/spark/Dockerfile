# OER Lakehouse - Spark Container
# ===============================

FROM bitnamilegacy/spark:3.5.4

USER root

# Install additional dependencies for Iceberg and OER processing
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Create symlink for /opt/spark to maintain compatibility
RUN ln -sf /opt/bitnami/spark /opt/spark

# Download Iceberg jars for Spark 3.5 directly to jars directory
RUN wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
    -O /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar
RUN wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.565/aws-java-sdk-bundle-1.12.565.jar \
    -O /opt/spark/jars/aws-java-sdk-bundle-1.12.565.jar
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -O /opt/spark/jars/hadoop-aws-3.3.4.jar

# Copy transformation scripts
COPY transformers/ /opt/spark/transformers/

USER 1001

