FROM tabulario/iceberg-rest:0.10.0

# Copy S3/MinIO support JARs to the lib directory where Iceberg REST loads dependencies
COPY ./hadoop-aws-3.3.4.jar /usr/lib/iceberg-rest/lib/
COPY ./aws-java-sdk-bundle-1.12.262.jar /usr/lib/iceberg-rest/lib/

# Override CMD to include our JARs in classpath
# Disable AWS SDK's default credential provider chain to force using environment variables
CMD ["java", "-Dcom.amazonaws.sdk.disableEc2Metadata=true", "-cp", "/usr/lib/iceberg-rest/iceberg-rest-image-all.jar:/usr/lib/iceberg-rest/lib/*", "org.apache.iceberg.rest.RESTCatalogServer"]
