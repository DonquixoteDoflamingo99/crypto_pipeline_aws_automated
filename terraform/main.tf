# Provider configuration
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# S3 Data Lake
resource "aws_s3_bucket" "crypto_data_lake" {
  bucket = "${var.project_name}-data-lake-${random_string.suffix.result}"
}

resource "aws_s3_bucket_versioning" "crypto_data_lake_versioning" {
  bucket = aws_s3_bucket.crypto_data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Kinesis Data Stream
resource "aws_kinesis_stream" "crypto_stream" {
  name             = "${var.project_name}-crypto-stream"
  shard_count      = 2
  retention_period = 168 # 7 days

  shard_level_metrics = [
    "IncomingRecords",
    "OutgoingRecords",
  ]

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}

# DynamoDB for real-time data
resource "aws_dynamodb_table" "crypto_prices" {
  name           = "${var.project_name}-crypto-prices"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "symbol"
  range_key      = "timestamp"

  attribute {
    name = "symbol"
    type = "S"
  }

  attribute {
    name = "timestamp"
    type = "N"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}

# EMR Cluster for Spark Streaming
resource "aws_emr_cluster" "crypto_processing" {
  name          = "${var.project_name}-emr-cluster"
  release_label = "emr-6.15.0"
  applications  = ["Spark", "Hadoop", "Hive"]
  
  service_role = aws_iam_role.emr_service_role.arn
  
  master_instance_group {
    instance_type = "m5.xlarge"
  }
  
  core_instance_group {
    instance_count = 2
    instance_type  = "m5.large"
  }
  
  ec2_attributes {
    subnet_id                         = aws_subnet.private.id
    emr_managed_master_security_group = aws_security_group.emr_master.id
    emr_managed_slave_security_group  = aws_security_group.emr_slave.id
    instance_profile                  = aws_iam_instance_profile.emr_profile.arn
  }
  
  auto_scaling_policy {
    constraints {
      min_capacity = 1
      max_capacity = 5
    }
    
    rules {
      name         = "ScaleOutMemoryPercentage"
      description  = "Scale out if YARNMemoryAvailablePercentage is less than 15"
      action {
        simple_scaling_policy_configuration {
          adjustment_type          = "CHANGE_IN_CAPACITY"
          scaling_adjustment       = 1
          cooldown                 = 300
        }
      }
      trigger {
        cloud_watch_alarm_definition {
          comparison_operator = "LESS_THAN"
          evaluation_periods  = 1
          metric_name        = "YARNMemoryAvailablePercentage"
          namespace          = "AWS/ElasticMapReduce"
          period             = 300
          statistic          = "AVERAGE"
          threshold          = 15.0
        }
      }
    }
  }
}

# Lambda for data ingestion
resource "aws_lambda_function" "crypto_ingestion" {
  filename      = "crypto_ingestion.zip"
  function_name = "${var.project_name}-crypto-ingestion"
  role          = aws_iam_role.lambda_role.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.11"
  timeout       = 60

  environment {
    variables = {
      KINESIS_STREAM_NAME = aws_kinesis_stream.crypto_stream.name
      DYNAMODB_TABLE_NAME = aws_dynamodb_table.crypto_prices.name
    }
  }
}

# CloudWatch Event Rule for Lambda trigger
resource "aws_cloudwatch_event_rule" "crypto_ingestion_schedule" {
  name                = "${var.project_name}-ingestion-schedule"
  description         = "Trigger crypto data ingestion every minute"
  schedule_expression = "rate(1 minute)"
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.crypto_ingestion_schedule.name
  target_id = "TriggerLambda"
  arn       = aws_lambda_function.crypto_ingestion.arn
}

resource "aws_lambda_permission" "allow_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.crypto_ingestion.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.crypto_ingestion_schedule.arn
}