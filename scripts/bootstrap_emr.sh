#!/bin/bash
# bootstrap_emr.sh - Install additional packages on EMR cluster

sudo pip3 install boto3==1.28.57
sudo pip3 install requests==2.31.0

# Install Kinesis connector for Spark
sudo aws s3 cp s3://spark-streaming-kinesis-asl/spark-streaming-kinesis-asl_2.12-3.4.0.jar /usr/lib/spark/jars/

# Configure Spark for optimal streaming
echo "spark.sql.streaming.metricsEnabled=true" >> /etc/spark/conf/spark-defaults.conf
echo "spark.sql.streaming.ui.enabled=true" >> /etc/spark/conf/spark-defaults.conf