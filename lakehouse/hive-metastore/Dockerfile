FROM apache/hive:4.0.1

# Download PostgreSQL JDBC driver directly using ADD
ADD https://jdbc.postgresql.org/download/postgresql-42.7.4.jar /opt/hive/lib/postgresql.jar

# Set proper permissions
USER root
RUN chmod 644 /opt/hive/lib/postgresql.jar
USER hive
