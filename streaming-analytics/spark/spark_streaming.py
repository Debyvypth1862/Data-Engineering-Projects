from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, expr
from pyspark.sql.types import StructType, StringType, TimestampType

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
    return SparkSession.builder \
        .appName("ClickstreamAnalytics") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config("spark.sql.shuffle.partitions", 4) \
        .getOrCreate()

def read_from_kafka(spark):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "clickstream") \
        .option("startingOffsets", "earliest") \
        .load()

def process_stream(df):
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
    
    # 1. URL popularity in 5-minute windows
    url_counts = parsed_df \
        .withWatermark("event_timestamp", "10 minutes") \
        .groupBy(
            window(col("event_timestamp"), "5 minutes"),
            "url"
        ) \
        .agg(count("*").alias("url_hits"))
    
    # 2. Device type distribution
    device_counts = parsed_df \
        .withWatermark("event_timestamp", "10 minutes") \
        .groupBy(
            window(col("event_timestamp"), "5 minutes"),
            "device"
        ) \
        .agg(count("*").alias("device_hits"))
    
    # 3. User activity patterns
    user_activity = parsed_df \
        .withWatermark("event_timestamp", "10 minutes") \
        .groupBy(
            window(col("event_timestamp"), "5 minutes"),
            "user_id"
        ) \
        .agg(count("*").alias("user_actions"))
    
    return url_counts, device_counts, user_activity

def write_stream(df, path, checkpoint_path):
    return df.writeStream \
        .format("json") \
        .option("path", path) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .start()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Read from Kafka
    kafka_df = read_from_kafka(spark)
    
    # Process the stream and get different aggregations
    url_counts, device_counts, user_activity = process_stream(kafka_df)
    
    # Write streams to different output locations
    queries = []
    base_path = "output"
    
    queries.append(write_stream(
        url_counts,
        f"{base_path}/url_counts",
        f"{base_path}/checkpoints/url_counts"
    ))
    
    queries.append(write_stream(
        device_counts,
        f"{base_path}/device_counts",
        f"{base_path}/checkpoints/device_counts"
    ))
    
    queries.append(write_stream(
        user_activity,
        f"{base_path}/user_activity",
        f"{base_path}/checkpoints/user_activity"
    ))
    
    # Wait for all queries to terminate
    for query in queries:
        query.awaitTermination()

if __name__ == "__main__":
    main()