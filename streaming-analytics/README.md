# Streaming Analytics with Kafka & Spark

A high-performance real-time data pipeline that processes over 10 million clickstream events using Apache Kafka and Apache Spark Streaming.

## Project Overview
- Generates realistic clickstream data (user interactions, page views, etc.)
- Streams data through Apache Kafka
- Processes streams with Apache Spark Streaming
- Produces real-time analytics on URL popularity, device usage, and user activity

## System Architecture

### Data Generation Layer
- Simulates 1000 unique users
- Generates diverse user actions (clicks, views, scrolls, hovers)
- Produces events at a rate of ~1000 events/second
- Each event includes:
  - Unique event ID (UUID)
  - User ID
  - URL/Page accessed
  - Action type
  - Timestamp
  - Session ID
  - Device type
  - Browser information

### Streaming Layer (Kafka)
- Single Kafka broker (can be scaled to multiple brokers)
- Topics:
  - `clickstream`: Main topic for raw event data
- Partitioning strategy: Default partitioning based on user_id
- Message format: JSON
- Throughput: Capable of handling 1000+ messages per second

### Processing Layer (Spark Streaming)
- Micro-batch processing with 1-minute triggers
- Watermarking: 10-minute delay tolerance
- Window operations:
  - 5-minute sliding windows for aggregations
  - Handles late-arriving data
- Processing guarantees: At-least-once delivery
- Parallel processing:
  - Multiple executors for distributed computation
  - Optimized shuffle partitions for better performance

### Storage Layer
- Output format: JSON files
- Directory structure:
  ```
  output/
  ├── url_counts/         # URL popularity metrics
  ├── device_counts/      # Device usage statistics
  └── user_activity/      # User behavior patterns
  ```
- Checkpoint directories for fault tolerance

## Performance Characteristics
- Data Generation: 1000 events/batch
- Processing Rate: ~10,000 events/second
- Total Processing Time: ~17 minutes for 10 million records
- Memory Usage: 
  - Kafka: 1-2GB heap
  - Spark: 4GB+ recommended
- Storage Requirements: ~2GB for full dataset

## Prerequisites
- Docker and Docker Compose
- Python 3.8+
- Apache Spark (local installation)
- Minimum 8GB RAM
- 10GB free disk space

## Setup

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Start Kafka and Zookeeper:
```bash
cd kafka
docker-compose up -d
```

3. Create output directories:
```bash
mkdir -p output/checkpoints/url_counts
mkdir -p output/checkpoints/device_counts
mkdir -p output/checkpoints/user_activity
```

## Running the Pipeline

1. Start the Kafka producer to generate clickstream data:
```bash
python kafka/producer.py
```

2. In a new terminal, start the Spark streaming job:
```bash
python spark/spark_streaming.py
```

## Monitoring Progress

### Producer Metrics
- Console output shows events generated per second
- Progress indicator for total events (out of 10 million)
- Batch completion timestamps

### Spark Metrics
The streaming job provides real-time statistics on:
- Processing rate (events/second)
- Input rate vs. processing rate
- Batch processing times
- Active tasks and executors

## Output Analytics
The pipeline generates three types of analytics in real-time:

1. URL Popularity (5-minute windows)
   - Most visited URLs
   - Peak traffic periods
   - URL access patterns

2. Device Type Distribution
   - Mobile vs. Desktop vs. Tablet usage
   - Device preferences by time window
   - Platform trends

3. User Activity Patterns
   - Active users per window
   - Session durations
   - User engagement metrics

Results are saved in JSON format in the `output` directory.

## Scalability
The system can be scaled by:
- Adding more Kafka partitions
- Increasing Spark executors
- Distributing Kafka brokers
- Adjusting batch sizes and processing windows

## Error Handling
- Automatic recovery from failures
- Checkpoint-based state management
- Dead letter queues for failed messages
- Robust error logging and monitoring

## Future Improvements
- Add monitoring dashboard (Grafana)
- Implement data quality checks
- Add real-time alerting
- Scale to distributed Kafka cluster
- Add data persistence layer (e.g., PostgreSQL)