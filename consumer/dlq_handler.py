import json
import boto3
import os
from datetime import datetime

S3_BUCKET = "priyanshu-stream-project"
REGION = "ap-south-1"

s3 = boto3.client('s3', region_name=REGION)

def send_to_dlq(event, reason):
    """
    Save failed events to S3 DLQ folder with failure reason.
    In production this would be SQS, but S3 DLQ works the same way conceptually.
    """
    dlq_record = {
        "failed_event": event,
        "failure_reason": reason,
        "failed_at": datetime.utcnow().isoformat(),
        "pipeline": "order-events-consumer"
    }

    now = datetime.utcnow()
    s3_key = f"dlq/year={now.year}/month={now.month:02d}/day={now.day:02d}/failed_{now.strftime('%H%M%S%f')}.json"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(dlq_record, indent=2)
    )
    print(f"DLQ: Sent failed event to s3://{S3_BUCKET}/{s3_key} | Reason: {reason}")