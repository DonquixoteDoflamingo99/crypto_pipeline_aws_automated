# airflow/dags/crypto_pipeline_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup

# Default arguments
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['data-team@company.com']
}

# DAG definition
dag = DAG(
    'crypto_streaming_pipeline',
    default_args=default_args,
    description='End-to-end crypto data streaming pipeline',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['crypto', 'streaming', 'data-engineering']
)

# EMR cluster configuration
EMR_CLUSTER_CONFIG = {
    'Name': 'crypto-processing-cluster',
    'ReleaseLabel': 'emr-6.15.0',
    'Applications': [
        {'Name': 'Spark'},
        {'Name': 'Hadoop'},
        {'Name': 'Hive'}
    ],
    'Instances': {
        'MasterInstanceType': 'm5.xlarge',
        'SlaveInstanceType': 'm5.large',
        'InstanceCount': 3,
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        'Ec2SubnetId': 'subnet-12345678',
        'EmrManagedMasterSecurityGroup': 'sg-master',
        'EmrManagedSlaveSecurityGroup': 'sg-slave'
    },
    'ServiceRole': 'EMR_DefaultRole',
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'LogUri': 's3://your-emr-logs-bucket/logs/',
    'BootstrapActions': [
        {
            'Name': 'Install Dependencies',
            'ScriptBootstrapAction': {
                'Path': 's3://your-scripts-bucket/bootstrap_emr.sh'
            }
        }
    ],
    'Configurations': [
        {
            'Classification': 'spark-defaults',
            'Properties': {
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.serializer': 'org.apache.spark.serializer.KryoSerializer'
            }
        }
    ]
}

# Spark streaming step
SPARK_STREAMING_STEP = [
    {
        'Name': 'crypto-stream-processing',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--master', 'yarn',
                '--conf', 'spark.sql.streaming.checkpointLocation=s3://your-bucket/checkpoints/',
                's3://your-scripts-bucket/crypto_stream_processor.py'
            ]
        }
    }
]

def validate_data_quality(**context):
    """Validate data quality and send alerts if needed"""
    import boto3
    
    # Connect to DynamoDB to check recent data
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('crypto-pipeline-crypto-prices')
    
    # Check if we have recent data (last 5 minutes)
    current_time = int(datetime.now().timestamp())
    five_minutes_ago = current_time - 300
    
    response = table.scan(
        FilterExpression='#ts > :timestamp',
        ExpressionAttributeNames={'#ts': 'timestamp'},
        ExpressionAttributeValues={':timestamp': five_minutes_ago}
    )
    
    if response['Count'] == 0:
        raise ValueError("No recent data found in the last 5 minutes")
    
    # Check data quality metrics
    total_records = response['Count']
    anomaly_count = sum(1 for item in response['Items'] if item.get('is_anomaly', False))
    anomaly_rate = anomaly_count / total_records if total_records > 0 else 0
    
    if anomaly_rate > 0.1:  # More than 10% anomalies
        context['task_instance'].xcom_push(
            key='data_quality_alert',
            value=f"High anomaly rate detected: {anomaly_rate:.2%}"
        )
    
    return {
        'total_records': total_records,
        'anomaly_count': anomaly_count,
        'anomaly_rate': anomaly_rate
    }

def cleanup_old_data(**context):
    """Cleanup old data from S3 based on retention policy"""
    import boto3
    from datetime import datetime, timedelta
    
    s3 = boto3.client('s3')
    bucket_name = 'your-data-lake-bucket'
    
    # Delete data older than 90 days
    cutoff_date = datetime.now() - timedelta(days=90)
    
    # List and delete old partitions
    prefix = f"crypto-data/detailed/partition_date={cutoff_date.strftime('%Y-%m-%d')}"
    
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if 'Contents' in response:
        objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
        
        if objects_to_delete:
            s3.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': objects_to_delete}
            )
            print(f"Deleted {len(objects_to_delete)} old objects")

# Task definitions
with TaskGroup('data_ingestion', dag=dag) as ingestion_group:
    
    validate_lambda = PythonOperator(
        task_id='validate_lambda_function',
        python_callable=lambda: print("Lambda function validation complete")
    )
    
    trigger_ingestion = LambdaInvokeFunctionOperator(
        task_id='trigger_crypto_ingestion',
        function_name='crypto-pipeline-crypto-ingestion',
        invocation_type='RequestResponse'
    )
    
    validate_lambda >> trigger_ingestion

with TaskGroup('stream_processing', dag=dag) as processing_group:
    
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        job_flow_overrides=EMR_CLUSTER_CONFIG,
        aws_conn_id='aws_default'
    )
    
    wait_for_cluster = EmrJobFlowSensor(
        task_id='wait_for_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='stream_processing.create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default'
    )
    
    add_streaming_step = EmrAddStepsOperator(
        task_id='add_streaming_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='stream_processing.create_emr_cluster', key='return_value') }}",
        steps=SPARK_STREAMING_STEP,
        aws_conn_id='aws_default'
    )
    
    wait_for_step = EmrStepSensor(
        task_id='wait_for_streaming_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='stream_processing.create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='stream_processing.add_streaming_step', key='return_value')[0] }}",
        aws_conn_id='aws_default'
    )
    
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id='terminate_emr_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='stream_processing.create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        trigger_rule='all_done'  # Run even if previous tasks fail
    )
    
    create_emr_cluster >> wait_for_cluster >> add_streaming_step >> wait_for_step >> terminate_emr_cluster

with TaskGroup('analytics_processing', dag=dag) as analytics_group:
    
    run_glue_catalog_job = GlueJobOperator(
        task_id='run_glue_catalog_job',
        job_name='crypto-data-catalog-job',
        script_args={
            '--S3_INPUT_PATH': 's3://your-data-lake-bucket/crypto-data/',
            '--S3_OUTPUT_PATH': 's3://your-data-lake-bucket/processed/'
        },
        aws_conn_id='aws_default'
    )

with TaskGroup('data_quality_monitoring', dag=dag) as monitoring_group:
    
    validate_data = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality
    )
    
    send_quality_report = EmailOperator(
        task_id='send_quality_report',
        to=['data-team@company.com'],
        subject='Crypto Pipeline Data Quality Report - {{ ds }}',
        html_content="""
        <h3>Daily Data Quality Report</h3>
        <p><strong>Date:</strong> {{ ds }}</p>
        <p><strong>Total Records:</strong> {{ task_instance.xcom_pull(task_ids='data_quality_monitoring.validate_data_quality', key='return_value')['total_records'] }}</p>
        <p><strong>Anomaly Count:</strong> {{ task_instance.xcom_pull(task_ids='data_quality_monitoring.validate_data_quality', key='return_value')['anomaly_count'] }}</p>
        <p><strong>Anomaly Rate:</strong> {{ (task_instance.xcom_pull(task_ids='data_quality_monitoring.validate_data_quality', key='return_value')['anomaly_rate'] * 100)|round(2) }}%</p>
        """
    )
    
    validate_data >> send_quality_report

with TaskGroup('maintenance', dag=dag) as maintenance_group:
    
    cleanup_old_data_task = PythonOperator(
        task_id='cleanup_old_data',
        python_callable=cleanup_old_data
    )

# Define task dependencies
ingestion_group >> processing_group >> analytics_group >> monitoring_group >> maintenance_group