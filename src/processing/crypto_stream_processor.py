# src/processing/crypto_stream_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import window, from_json, col, current_timestamp, to_timestamp, from_unixtime, lag, stddev, avg, when, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType
from pyspark.sql.window import Window
import json

class CryptoStreamProcessor:
    def __init__(self):
        self.spark = self.create_spark_session()
        self.setup_streaming_parameters()
    
    def create_spark_session(self):
        """Create Spark session with required configurations"""
        return SparkSession.builder \
            .appName("CryptoStreamProcessor") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.streaming.checkpointLocation", "s3a://your-bucket/checkpoints/") \
            .getOrCreate()
    
    def setup_streaming_parameters(self):
        """Setup streaming configuration parameters"""
        self.kinesis_stream_name = "crypto-pipeline-crypto-stream"
        self.kinesis_region = "us-east-1"
        self.s3_output_path = "s3a://your-data-lake-bucket/crypto-data/"
        self.dynamodb_table = "crypto-pipeline-crypto-prices"
    
    def define_schema(self):
        """Define schema for incoming crypto data"""
        return StructType([
            StructField("symbol", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("volume", DoubleType(), True),
            StructField("high_24h", DoubleType(), True),
            StructField("low_24h", DoubleType(), True),
            StructField("price_change_24h", DoubleType(), True),
            StructField("price_change_percent_24h", DoubleType(), True),
            StructField("trade_count", IntegerType(), True),
            StructField("timestamp", LongType(), True),
            StructField("source", StringType(), True),
            StructField("market_cap", DoubleType(), True),
            StructField("volume_24h_usd", DoubleType(), True),
            StructField("event_time", StringType(), True),
            StructField("partition_date", StringType(), True),
            StructField("partition_hour", StringType(), True)
        ])
    
    def read_from_kinesis(self):
        """Read streaming data from Kinesis"""
        kinesis_df = self.spark \
            .readStream \
            .format("kinesis") \
            .option("streamName", self.kinesis_stream_name) \
            .option("region", self.kinesis_region) \
            .option("initialPosition", "TRIM_HORIZON") \
            .option("awsAccessKeyId", "YOUR_ACCESS_KEY") \
            .option("awsSecretKey", "YOUR_SECRET_KEY") \
            .load()
        
        # Parse JSON data
        schema = self.define_schema()
        
        parsed_df = kinesis_df \
            .select(from_json(col("data").cast("string"), schema).alias("crypto")) \
            .select("crypto.*") \
            .withColumn("processing_time", current_timestamp()) \
            .withColumn("watermark_time", 
                       to_timestamp(from_unixtime(col("timestamp"))))
        
        return parsed_df
    
    def calculate_technical_indicators(self, df):
        """Calculate technical indicators using window functions"""
        
        # Define windows for calculations
        symbol_window = Window.partitionBy("symbol").orderBy("timestamp")
        symbol_time_window = Window.partitionBy("symbol") \
            .orderBy("timestamp") \
            .rowsBetween(-19, 0)  # 20-period window
        
        enriched_df = df \
            .withColumn("price_lag_1", lag("price", 1).over(symbol_window)) \
            .withColumn("price_change", col("price") - col("price_lag_1")) \
            .withColumn("price_change_pct", 
                       (col("price_change") / col("price_lag_1")) * 100) \
            .withColumn("sma_20", avg("price").over(symbol_time_window)) \
            .withColumn("price_volatility", 
                       stddev("price_change_pct").over(symbol_time_window)) \
            .withColumn("volume_sma_20", avg("volume").over(symbol_time_window))
        
        # Calculate RSI (Relative Strength Index)
        enriched_df = self.calculate_rsi(enriched_df, symbol_window)
        
        # Calculate MACD
        enriched_df = self.calculate_macd(enriched_df, symbol_window)
        
        # Detect price anomalies
        enriched_df = self.detect_anomalies(enriched_df, symbol_time_window)
        
        return enriched_df
    
    def calculate_rsi(self, df, window_spec):
        """Calculate RSI indicator"""
        # Calculate gains and losses
        rsi_df = df \
            .withColumn("gain", when(col("price_change") > 0, col("price_change")).otherwise(0)) \
            .withColumn("loss", when(col("price_change") < 0, -col("price_change")).otherwise(0))
        
        # Calculate average gains and losses over 14 periods
        window_14 = window_spec.rowsBetween(-13, 0)
        
        rsi_df = rsi_df \
            .withColumn("avg_gain", avg("gain").over(window_14)) \
            .withColumn("avg_loss", avg("loss").over(window_14)) \
            .withColumn("rs", col("avg_gain") / col("avg_loss")) \
            .withColumn("rsi", 100 - (100 / (1 + col("rs"))))
        
        return rsi_df
    
    def calculate_macd(self, df, window_spec):
        """Calculate MACD indicator"""
        # Calculate EMAs
        ema_12_window = window_spec.rowsBetween(-11, 0)
        ema_26_window = window_spec.rowsBetween(-25, 0)
        
        macd_df = df \
            .withColumn("ema_12", avg("price").over(ema_12_window)) \
            .withColumn("ema_26", avg("price").over(ema_26_window)) \
            .withColumn("macd_line", col("ema_12") - col("ema_26"))
        
        # Calculate signal line (9-period EMA of MACD)
        signal_window = window_spec.rowsBetween(-8, 0)
        macd_df = macd_df \
            .withColumn("macd_signal", avg("macd_line").over(signal_window)) \
            .withColumn("macd_histogram", col("macd_line") - col("macd_signal"))
        
        return macd_df
    
    def detect_anomalies(self, df, window_spec):
        """Detect price anomalies using statistical methods"""
        anomaly_df = df \
            .withColumn("price_mean", avg("price").over(window_spec)) \
            .withColumn("price_stddev", stddev("price").over(window_spec)) \
            .withColumn("z_score", 
                       (col("price") - col("price_mean")) / col("price_stddev")) \
            .withColumn("is_anomaly", abs(col("z_score")) > 2.5) \
            .withColumn("anomaly_type", 
                       when(col("z_score") > 2.5, "price_spike")
                       .when(col("z_score") < -2.5, "price_drop")
                       .otherwise("normal"))
        
        return anomaly_df
    
    def aggregate_market_data(self, df):
        """Create market-wide aggregations"""
        # Window for time-based aggregations
        time_window = window(col("watermark_time"), "5 minutes")
        
        market_agg_df = df \
            .withWatermark("watermark_time", "10 minutes") \
            .groupBy(time_window, "symbol") \
            .agg(
                avg("price").alias("avg_price_5min"),
                max("price").alias("max_price_5min"),
                min("price").alias("min_price_5min"),
                sum("volume").alias("total_volume_5min"),
                count("*").alias("trade_count_5min"),
                avg("rsi").alias("avg_rsi_5min"),
                max("is_anomaly").alias("has_anomaly_5min")
            ) \
            .withColumn("price_range_5min", 
                       col("max_price_5min") - col("min_price_5min")) \
            .withColumn("volatility_5min", 
                       col("price_range_5min") / col("avg_price_5min") * 100)
        
        return market_agg_df
    
    def write_to_s3(self, df, output_path, format_type="parquet"):
        """Write processed data to S3 data lake"""
        query = df \
            .writeStream \
            .format(format_type) \
            .option("path", output_path) \
            .option("checkpointLocation", f"{output_path}_checkpoint/") \
            .partitionBy("partition_date", "symbol") \
            .trigger(processingTime='30 seconds') \
            .start()
        
        return query
    
    def write_to_dynamodb(self, df):
        """Write real-time data to DynamoDB"""
        def write_to_dynamo_batch(batch_df, batch_id):
            """Function to write each micro-batch to DynamoDB"""
            import boto3
            from boto3.dynamodb.conditions import Key
            
            dynamodb = boto3.resource('dynamodb', region_name=self.kinesis_region)
            table = dynamodb.Table(self.dynamodb_table)
            
            # Convert to pandas for easier processing
            pandas_df = batch_df.toPandas()
            
            with table.batch_writer() as batch:
                for _, row in pandas_df.iterrows():
                    item = {
                        'symbol': row['symbol'],
                        'timestamp': int(row['timestamp']),
                        'price': float(row['price']),
                        'rsi': float(row['rsi']) if row['rsi'] else None,
                        'macd_line': float(row['macd_line']) if row['macd_line'] else None,
                        'is_anomaly': bool(row['is_anomaly']),
                        'processing_time': row['processing_time'].isoformat(),
                        'ttl': int(row['timestamp']) + 86400  # 24 hours TTL
                    }
                    batch.put_item(Item=item)
        
        query = df.select("symbol", "timestamp", "price", "rsi", "macd_line", 
                         "is_anomaly", "processing_time") \
            .writeStream \
            .foreachBatch(write_to_dynamo_batch) \
            .trigger(processingTime='60 seconds') \
            .start()
        
        return query
    
    def run_streaming_pipeline(self):
        """Main method to run the streaming pipeline"""
        print("Starting Crypto Stream Processor...")
        
        # Read from Kinesis
        raw_stream = self.read_from_kinesis()
        
        # Calculate technical indicators
        enriched_stream = self.calculate_technical_indicators(raw_stream)
        
        # Create market aggregations
        market_agg_stream = self.aggregate_market_data(enriched_stream)
        
        # Write detailed data to S3
        s3_query = self.write_to_s3(
            enriched_stream, 
            f"{self.s3_output_path}detailed/"
        )
        
        # Write aggregated data to S3
        s3_agg_query = self.write_to_s3(
            market_agg_stream, 
            f"{self.s3_output_path}aggregated/"
        )
        
        # Write real-time data to DynamoDB
        dynamo_query = self.write_to_dynamodb(enriched_stream)
        
        # Wait for termination
        s3_query.awaitTermination()

if __name__ == "__main__":
    processor = CryptoStreamProcessor()
    processor.run_streaming_pipeline()