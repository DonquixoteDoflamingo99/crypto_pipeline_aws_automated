# src/monitoring/custom_metrics.py
import boto3
import json
from datetime import datetime

class CryptoMetricsCollector:
    def __init__(self):
        self.cloudwatch = boto3.client('cloudwatch')
        self.dynamodb = boto3.resource('dynamodb')
        
    def publish_custom_metrics(self):
        """Publish custom metrics to CloudWatch"""
        
        # Get current data from DynamoDB
        table = self.dynamodb.Table('crypto-pipeline-crypto-prices')
        
        current_time = int(datetime.now().timestamp())
        five_minutes_ago = current_time - 300
        
        response = table.scan(
            FilterExpression='#ts > :timestamp',
            ExpressionAttributeNames={'#ts': 'timestamp'},
            ExpressionAttributeValues={':timestamp': five_minutes_ago}
        )
        
        # Calculate metrics
        total_records = response['Count']
        anomaly_count = sum(1 for item in response['Items'] if item.get('is_anomaly', False))
        symbols_count = len(set(item['symbol'] for item in response['Items']))
        
        # Publish metrics
        metrics = [
            {
                'MetricName': 'TotalRecords',
                'Value': total_records,
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'Pipeline', 'Value': 'CryptoPipeline'}
                ]
            },
            {
                'MetricName': 'AnomalyCount',
                'Value': anomaly_count,
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'Pipeline', 'Value': 'CryptoPipeline'}
                ]
            },
            {
                'MetricName': 'AnomalyRate',
                'Value': (anomaly_count / total_records * 100) if total_records > 0 else 0,
                'Unit': 'Percent',
                'Dimensions': [
                    {'Name': 'Pipeline', 'Value': 'CryptoPipeline'}
                ]
            },
            {
                'MetricName': 'ActiveSymbols',
                'Value': symbols_count,
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'Pipeline', 'Value': 'CryptoPipeline'}
                ]
            }
        ]
        
        # Publish all metrics
        self.cloudwatch.put_metric_data(
            Namespace='CryptoPipeline',
            MetricData=metrics
        )
        
        print(f"Published {len(metrics)} custom metrics to CloudWatch")

if __name__ == "__main__":
    collector = CryptoMetricsCollector()
    collector.publish_custom_metrics()