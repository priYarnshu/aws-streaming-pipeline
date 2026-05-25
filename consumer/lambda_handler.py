import json
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import os
from kafka import KafkaConsumer
from datetime import datetime

TOPIC_NAME = "order-events"
KAFKA_SERVER = "localhost:9092"
S3_BUCKET = "priyanshu-stream-project"
REGION = "ap-south-1"

s3 = boto3.client('s3', region_name=REGION)

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='order-consumer-group'
)

def validate_event(event):
    required_fields = [
        "order_id", "customer_id", "product_name",
        "total_amount", "status", "event_timestamp"
    ]
    for field in required_fields:
        if field not in event or event[field] is None:
            return False
    if event["total_amount"] <= 0:
        return False
    return True

def transform_event(event):
    event["processed_at"] = datetime.utcnow().isoformat()
    event["total_amount"] = float(event["total_amount"])
    event["unit_price"] = float(event["unit_price"])
    event["quantity"] = int(event["quantity"])
    event["is_high_value"] = event["total_amount"] > 10000
    return event

def upload_to_s3(events):
    df = pd.DataFrame(events)
    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    now = datetime.utcnow()
    s3_key = f"orders/year={now.year}/month={now.month:02d}/day={now.day:02d}/orders_{now.strftime('%H%M%S')}.parquet"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue()
    )
    print(f"Uploaded {len(events)} events to s3://{S3_BUCKET}/{s3_key}")

if __name__ == "__main__":
    print(f"Starting consumer - reading from topic '{TOPIC_NAME}'...")
    print("Will upload to S3 every 10 events\n")

    batch = []
    valid_count = 0
    invalid_count = 0

    for message in consumer:
        event = message.value

        if not validate_event(event):
            invalid_count += 1
            print(f"Invalid event skipped | total invalid: {invalid_count}")
            continue

        event = transform_event(event)
        batch.append(event)
        valid_count += 1
        print(f"[{valid_count}] Consumed order {event['order_id']} | {event['product_name']} | Rs.{event['total_amount']} | High value: {event['is_high_value']}")

        if len(batch) >= 10:
            upload_to_s3(batch)
            batch = []