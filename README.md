# Real-Time E-Commerce Streaming Pipeline

A production-grade real-time data pipeline that processes e-commerce order events 
using Apache Kafka and AWS (S3, Glue, Athena).

## Architecture
Order Events → Apache Kafka → Python Consumer → S3 (Parquet) → Glue Catalog → Athena

## Tech Stack
- **Apache Kafka** — real-time event streaming and message broker
- **Python / kafka-python** — producer and consumer scripts
- **AWS S3** — data lake storage in partitioned Parquet format
- **AWS Glue** — automated schema discovery and data catalog
- **AWS Athena** — serverless SQL queries on S3 data
- **pandas / pyarrow** — data transformation and Parquet serialization

## Pipeline Features
- Generates realistic Indian e-commerce order events in real time
- Validates and transforms events before landing to S3
- Partitions data by year/month/day for query performance
- Detects high-value orders (> Rs.10,000) during stream processing
- Supports SQL analytics via Athena on raw S3 Parquet files
- Dead Letter Queue for failed/invalid events with failure reason logging
- CloudWatch custom metrics tracking events processed, failed, and S3 uploads
- CloudWatch alarm triggering when DLQ rate exceeds 5 events per 5 minutes


## Project Structure
```
aws-streaming-pipeline/
├── producer/
│   └── order_producer.py      # Kafka producer - generates order events
├── consumer/
│   └── lambda_handler.py      # Kafka consumer - transforms and lands to S3
├── infrastructure/            # IaC scripts (coming soon)
└── README.md
```

## Setup Instructions

### Prerequisites
- Python 3.9+
- Apache Kafka
- AWS account with S3, Glue, Athena access

### Install dependencies
```bash
pip install kafka-python boto3 faker pandas pyarrow
```

### Configure AWS
```bash
aws configure
```

### Start Kafka
```bash
brew services start kafka
```

### Create Kafka topic
```bash
kafka-topics --create \
  --topic order-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### Run the pipeline
Terminal 1 - start producer:
```bash
python producer/order_producer.py
```

Terminal 2 - start consumer:
```bash
python consumer/lambda_handler.py
```

## Sample Athena Queries

### Revenue by category
```sql
SELECT 
    category,
    COUNT(*) as total_orders,
    SUM(total_amount) as total_revenue,
    ROUND(AVG(total_amount), 2) as avg_order_value
FROM orders
GROUP BY category
ORDER BY total_revenue DESC;
```

### Orders by city
```sql
SELECT 
    customer_city,
    COUNT(*) as total_orders,
    SUM(total_amount) as revenue
FROM orders
GROUP BY customer_city
ORDER BY revenue DESC;
```

## Key Metrics
- Processes 1 event/second continuously
- Batches 10 events per S3 upload
- Data partitioned by year/month/day
- Supports SQL analytics on raw Parquet files via Athena

