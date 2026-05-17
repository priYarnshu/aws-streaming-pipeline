import boto3
import json
import random
import time
from faker import Faker
from datetime import datetime

fake = Faker('en_IN')

STREAM_NAME = "order-events-stream"
REGION = "ap-south-1"

kinesis = boto3.client('kinesis', region_name=REGION)

PRODUCTS = [
    {"name": "iPhone 15", "category": "Electronics", "price": 79999},
    {"name": "Nike Air Max", "category": "Footwear", "price": 8999},
    {"name": "Samsung TV 55inch", "category": "Electronics", "price": 54999},
    {"name": "Levis Jeans", "category": "Clothing", "price": 2999},
    {"name": "Dyson Vacuum", "category": "Appliances", "price": 34999},
    {"name": "Harry Potter Set", "category": "Books", "price": 1999},
    {"name": "Yoga Mat", "category": "Fitness", "price": 1499},
    {"name": "Coffee Maker", "category": "Appliances", "price": 4999},
]

STATUSES = ["PLACED", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]
PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "COD"]
CITIES = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Pune", "Gurugram"]

def generate_order_event():
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 3)
    return {
        "order_id": f"ORD-{fake.uuid4()[:8].upper()}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "customer_name": fake.name(),
        "customer_city": random.choice(CITIES),
        "product_name": product["name"],
        "category": product["category"],
        "quantity": quantity,
        "unit_price": product["price"],
        "total_amount": product["price"] * quantity,
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": random.choice(STATUSES),
        "event_timestamp": datetime.utcnow().isoformat(),
    }

def send_to_kinesis(event):
    response = kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(event),
        PartitionKey=event["customer_id"]
    )
    return response

if __name__ == "__main__":
    print(f"Starting producer - sending to '{STREAM_NAME}'...")
    print("Press Ctrl+C to stop\n")

    count = 0
    while True:
        event = generate_order_event()
        response = send_to_kinesis(event)
        count += 1
        print(f"[{count}] Sent order {event['order_id']} | {event['product_name']} | Rs.{event['total_amount']} | Shard: {response['ShardId']}")
        time.sleep(1)