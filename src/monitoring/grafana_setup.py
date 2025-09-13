# src/monitoring/grafana_setup.py
import requests
import json
import os

class GrafanaConfigurator:
    def __init__(self, grafana_url, api_key):
        self.grafana_url = grafana_url
        self.headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
    
    def setup_datasources(self):
        """Configure CloudWatch and DynamoDB data sources"""
        
        # CloudWatch data source
        cloudwatch_datasource = {
            "name": "CloudWatch",
            "type": "cloudwatch",
            "access": "proxy",
            "jsonData": {
                "defaultRegion": "us-east-1",
                "authType": "credentials"
            },
            "secureJsonData": {
                "accessKey": os.environ.get('AWS_ACCESS_KEY_ID'),
                "secretKey": os.environ.get('AWS_SECRET_ACCESS_KEY')
            }
        }
        
        # DynamoDB data source (via custom plugin)
        dynamodb_datasource = {
            "name": "DynamoDB",
            "type": "dynamodb-datasource",
            "access": "proxy",
            "jsonData": {
                "region": "us-east-1",
                "authType": "credentials"
            },
            "secureJsonData": {
                "accessKey": os.environ.get('AWS_ACCESS_KEY_ID'),
                "secretKey": os.environ.get('AWS_SECRET_ACCESS_KEY')
            }
        }
        
        # Create data sources
        for datasource in [cloudwatch_datasource, dynamodb_datasource]:
            response = requests.post(
                f"{self.grafana_url}/api/datasources",
                headers=self.headers,
                data=json.dumps(datasource)
            )
            print(f"Created datasource {datasource['name']}: {response.status_code}")
    
    def create_alerts(self):
        """Create Grafana alerts for monitoring"""
        
        alerts = [
            {
                "title": "High Anomaly Rate Alert",
                "message": "Crypto pipeline showing high anomaly rate",
                "frequency": "10m",
                "conditions": [
                    {
                        "query": {
                            "queryType": "",
                            "refId": "A",
                            "datasourceUid": "cloudwatch",
                            "model": {
                                "metricName": "AnomalyRate",
                                "namespace": "CryptoPipeline",
                                "statistic": "Average"
                            }
                        },
                        "reducer": {
                            "type": "last",
                            "params": []
                        },
                        "evaluator": {
                            "params": [10],
                            "type": "gt"
                        }
                    }
                ],
                "executionErrorState": "alerting",
                "noDataState": "no_data",
                "forDuration": "5m"
            },
            {
                "title": "Pipeline Data Gap Alert",
                "message": "No recent data in crypto pipeline",
                "frequency": "5m",
                "conditions": [
                    {
                        "query": {
                            "queryType": "",
                            "refId": "A",
                            "datasourceUid": "cloudwatch",
                            "model": {
                                "metricName": "TotalRecords",
                                "namespace": "CryptoPipeline",
                                "statistic": "Sum"
                            }
                        },
                        "reducer": {
                            "type": "last",
                            "params": []
                        },
                        "evaluator": {
                            "params": [1],
                            "type": "lt"
                        }
                    }
                ],
                "executionErrorState": "alerting",
                "noDataState": "alerting",
                "forDuration": "5m"
            }
        ]
        
        for alert in alerts:
            response = requests.post(
                f"{self.grafana_url}/api/alerts",
                headers=self.headers,
                data=json.dumps(alert)
            )
            print(f"Created alert {alert['title']}: {response.status_code}")

if __name__ == "__main__":
    configurator = GrafanaConfigurator(
        grafana_url=os.environ.get('GRAFANA_URL'),
        api_key=os.environ.get('GRAFANA_API_KEY')
    )
    configurator.setup_datasources()
    configurator.create_alerts()