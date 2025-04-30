import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, expr
from pyspark.sql.types import StructType, StringType, TimestampType

# Enhanced logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the schema for our clickstream data
schema = StructType() \
    .add("event_id", StringType()) \
    .add("user_id", StringType()) \
    .add("url", StringType()) \
    .add("action", StringType()) \
    .add("timestamp", StringType()) \
    .add("session_id", StringType()) \
    .add("device", StringType()) \
    .add("browser", StringType())

def create_spark_session():
    logger.info("Creating Spark session...")
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
        "org.apache.kafka:kafka-clients:3.4.0"
    ]
    
    builder = SparkSession.builder \
        .appName("ClickstreamAnalytics") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config("spark.sql.shuffle.partitions", 4) \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.streaming.checkpointLocation", "/app/checkpoints") \
        .master("local[*]")

    spark = builder.getOrCreate()
    logger.info("Spark session created successfully")
    return spark

def read_from_kafka(spark):
    logger.info("Attempting to connect to Kafka...")
    try:
        # Use the internal Kafka address from docker-compose
        kafka_bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", "clickstream") \
            .option("startingOffsets", "earliest") \
            .load()
        logger.info("Successfully connected to Kafka")
        return df
    except Exception as e:
        logger.error(f"Error connecting to Kafka: {str(e)}")
        raise

def process_stream(df):
    logger.info("Processing stream...")
    try:
        # Parse JSON and select fields
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("parsed_value")
        ).select("parsed_value.*")
        
        # Convert string timestamp to timestamp type
        parsed_df = parsed_df.withColumn(
            "event_timestamp", 
            expr("cast(timestamp as timestamp)")
        )
        
        # Create various aggregations
        url_counts = parsed_df \
            .withWatermark("event_timestamp", "10 minutes") \
            .groupBy(
                window(col("event_timestamp"), "5 minutes"),
                "url"
            ) \
            .agg(count("*").alias("url_hits"))
        
        device_counts = parsed_df \
            .withWatermark("event_timestamp", "10 minutes") \
            .groupBy(
                window(col("event_timestamp"), "5 minutes"),
                "device"
            ) \
            .agg(count("*").alias("device_hits"))
        
        user_activity = parsed_df \
            .withWatermark("event_timestamp", "10 minutes") \
            .groupBy(
                window(col("event_timestamp"), "5 minutes"),
                "user_id"
            ) \
            .agg(count("*").alias("user_actions"))
        
        return url_counts, device_counts, user_activity
    except Exception as e:
        logger.error(f"Error processing stream: {str(e)}")
        raise

def write_stream(df, path, checkpoint_path):
    logger.info(f"Setting up stream writer for {path}")
    try:
        return df.writeStream \
            .format("json") \
            .option("path", path) \
            .option("checkpointLocation", checkpoint_path) \
            .outputMode("append") \
            .trigger(processingTime="1 minute") \
            .start()
    except Exception as e:
        logger.error(f"Error setting up stream writer: {str(e)}")
        raise

def main():
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("INFO")
        
        logger.info("Starting streaming pipeline...")
        
        # Read from Kafka
        kafka_df = read_from_kafka(spark)
        
        # Process the stream and get different aggregations
        url_counts, device_counts, user_activity = process_stream(kafka_df)
        
        # Write streams to different output locations using Docker container paths
        queries = []
        base_path = "/app/output"
        
        logger.info(f"Writing output to {base_path}")
        
        queries.append(write_stream(
            url_counts,
            f"{base_path}/url_counts",
            "/app/checkpoints/url_counts"
        ))
        
        queries.append(write_stream(
            device_counts,
            f"{base_path}/device_counts",
            "/app/checkpoints/device_counts"
        ))
        
        queries.append(write_stream(
            user_activity,
            f"{base_path}/user_activity",
            "/app/checkpoints/user_activity"
        ))
        
        logger.info("All streams initialized. Waiting for termination...")
        
        # Wait for all queries to terminate
        for query in queries:
            query.awaitTermination()
            
    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
        raise

if __name__ == "__main__":
    main()