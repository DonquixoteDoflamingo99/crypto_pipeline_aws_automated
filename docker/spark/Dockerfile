# docker/spark/Dockerfile
FROM apache/spark:3.4.0-scala2.12-java11-python3-ubuntu

USER root

# Install required Python packages
RUN pip install --no-cache-dir \
    boto3==1.28.57 \
    requests==2.31.0 \
    pandas==2.0.3 \
    numpy==1.24.3 \
    scipy==1.11.2

# Copy application code
COPY src/processing/ /opt/spark/apps/
COPY src/monitoring/ /opt/spark/monitoring/

# Set working directory
WORKDIR /opt/spark/apps

# Add Kinesis connector
RUN wget -O /opt/spark/jars/spark-streaming-kinesis-asl_2.12-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kinesis-asl_2.12/3.4.0/spark-streaming-kinesis-asl_2.12-3.4.0.jar

USER 185

ENTRYPOINT ["/opt/entrypoint.sh"]