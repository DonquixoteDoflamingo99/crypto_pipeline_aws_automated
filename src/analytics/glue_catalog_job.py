# src/analytics/glue_catalog_job.py
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import when, col, avg, max, min, sum, count, countDistinct

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_OUTPUT_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

class CryptoDataCatalogJob:
    def __init__(self, glue_context, s3_input_path, s3_output_path):
        self.glue_context = glue_context
        self.s3_input_path = s3_input_path
        self.s3_output_path = s3_output_path
    
    def create_crypto_catalog(self):
        """Create and optimize data catalog for crypto data"""
        
        # Read raw crypto data
        crypto_data = self.glue_context.create_dynamic_frame.from_options(
            format_options={"multiline": False},
            connection_type="s3",
            format="parquet",
            connection_options={
                "paths": [f"{self.s3_input_path}detailed/"],
                "recurse": True
            },
            transformation_ctx="crypto_data"
        )
        
        # Data quality checks and cleaning
        cleaned_data = self.clean_and_validate_data(crypto_data)
        
        # Create optimized partitioned tables
        self.create_daily_partitions(cleaned_data)
        self.create_symbol_analytics_table(cleaned_data)
        self.create_market_summary_table()
        
        # Create Glue Data Catalog tables
        self.register_catalog_tables()
    
    def clean_and_validate_data(self, dynamic_frame):
        """Clean and validate crypto data"""
        
        # Convert to DataFrame for complex operations
        df = dynamic_frame.toDF()
        
        # Data quality checks
        df_cleaned = df.filter(
            (df.price > 0) & 
            (df.volume >= 0) & 
            (df.timestamp.isNotNull()) &
            (df.symbol.isNotNull())
        )
        
        # Remove duplicates
        df_cleaned = df_cleaned.dropDuplicates(['symbol', 'timestamp'])
        
        # Add data quality metrics
        df_cleaned = df_cleaned.withColumn(
            "data_quality_score",
            when(col("rsi").isNotNull() & col("macd_line").isNotNull(), 100)
            .when(col("price").isNotNull() & col("volume").isNotNull(), 80)
            .otherwise(60)
        )
        
        return DynamicFrame.fromDF(df_cleaned, self.glue_context, "cleaned_data")
    
    def create_daily_partitions(self, dynamic_frame):
        """Create daily partitioned tables for efficient querying"""
        
        # Write partitioned data optimized for analytics
        self.glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            format="parquet",
            connection_options={
                "path": f"{self.s3_output_path}crypto_daily/",
                "partitionKeys": ["partition_date", "symbol"]
            },
            format_options={
                "compression": "snappy",
                "blockSize": 134217728,  # 128MB blocks
                "pageSize": 1048576      # 1MB pages
            },
            transformation_ctx="daily_partitions"
        )
    
    def create_symbol_analytics_table(self, dynamic_frame):
        """Create symbol-specific analytics tables"""
        
        df = dynamic_frame.toDF()
        
        # Create hourly aggregations by symbol
        hourly_agg = df.groupBy("symbol", "partition_date", "partition_hour") \
            .agg(
                avg("price").alias("avg_price"),
                max("price").alias("max_price"),
                min("price").alias("min_price"),
                sum("volume").alias("total_volume"),
                avg("rsi").alias("avg_rsi"),
                avg("macd_line").alias("avg_macd"),
                sum(when(col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
                count("*").alias("data_points")
            ) \
            .withColumn("price_volatility", 
                       (col("max_price") - col("min_price")) / col("avg_price") * 100)
        
        # Write hourly aggregations
        hourly_dynamic_frame = DynamicFrame.fromDF(hourly_agg, self.glue_context, "hourly_agg")
        
        self.glue_context.write_dynamic_frame.from_options(
            frame=hourly_dynamic_frame,
            connection_type="s3",
            format="parquet",
            connection_options={
                "path": f"{self.s3_output_path}crypto_hourly_agg/",
                "partitionKeys": ["partition_date", "symbol"]
            },
            transformation_ctx="hourly_aggregations"
        )
    
    def create_market_summary_table(self):
        """Create market-wide summary tables"""
        
        # Read aggregated data
        agg_data = self.glue_context.create_dynamic_frame.from_options(
            connection_type="s3",
            format="parquet",
            connection_options={
                "paths": [f"{self.s3_input_path}aggregated/"]
            }
        )
        
        df = agg_data.toDF()
        
        # Create market summary
        market_summary = df.groupBy("window") \
            .agg(
                avg("avg_price_5min").alias("market_avg_price"),
                sum("total_volume_5min").alias("total_market_volume"),
                avg("volatility_5min").alias("avg_market_volatility"),
                countDistinct("symbol").alias("active_symbols"),
                sum(when(col("has_anomaly_5min"), 1).otherwise(0)).alias("total_anomalies")
            )
        
        # Write market summary
        market_dynamic_frame = DynamicFrame.fromDF(market_summary, self.glue_context, "market_summary")
        
        self.glue_context.write_dynamic_frame.from_options(
            frame=market_dynamic_frame,
            connection_type="s3",
            format="parquet",
            connection_options={
                "path": f"{self.s3_output_path}market_summary/"
            },
            transformation_ctx="market_summary"
        )
    
    def register_catalog_tables(self):
        """Register tables in Glue Data Catalog"""
        
        # This would typically be done through Terraform or AWS CLI
        # Here's the structure for reference
        table_definitions = {
            "crypto_detailed": {
                "location": f"{self.s3_output_path}crypto_daily/",
                "inputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "outputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "serdeInfo": {
                    "serializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                },
                "partitionKeys": [
                    {"name": "partition_date", "type": "string"},
                    {"name": "symbol", "type": "string"}
                ]
            }
        }

# Run the job
catalog_job = CryptoDataCatalogJob(
    glueContext, 
    args['S3_INPUT_PATH'], 
    args['S3_OUTPUT_PATH']
)
catalog_job.create_crypto_catalog()

job.commit()