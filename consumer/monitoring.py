import boto3
from datetime import datetime

REGION = "ap-south-1"

cloudwatch = boto3.client('cloudwatch', region_name=REGION)

def put_metric(metric_name, value, unit="Count"):
    """
    Push a custom metric to CloudWatch.
    """
    cloudwatch.put_metric_data(
        Namespace='EcommerceStreamingPipeline',
        MetricData=[
            {
                'MetricName': metric_name,
                'Value': value,
                'Unit': unit,
                'Timestamp': datetime.utcnow(),
                'Dimensions': [
                    {
                        'Name': 'Pipeline',
                        'Value': 'order-events-consumer'
                    }
                ]
            }
        ]
    )

def record_event_processed():
    put_metric('EventsProcessed', 1)

def record_event_failed():
    put_metric('EventsFailed', 1)

def record_s3_upload(event_count):
    put_metric('S3UploadsSuccess', 1)
    put_metric('EventsBatchedToS3', event_count)

def create_dlq_alarm():
    """
    Create a CloudWatch alarm - alerts if more than 5 events
    fail in 5 minutes. This is what you'd set up in production.
    """
    cloudwatch.put_metric_alarm(
        AlarmName='HighDLQRate-OrderPipeline',
        AlarmDescription='Too many events failing validation',
        MetricName='EventsFailed',
        Namespace='EcommerceStreamingPipeline',
        Statistic='Sum',
        Period=300,
        EvaluationPeriods=1,
        Threshold=5,
        ComparisonOperator='GreaterThanThreshold',
        Dimensions=[
            {
                'Name': 'Pipeline',
                'Value': 'order-events-consumer'
            }
        ],
        TreatMissingData='notBreaching'
    )
    print("CloudWatch alarm created: HighDLQRate-OrderPipeline")