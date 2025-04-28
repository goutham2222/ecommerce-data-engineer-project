import json
import boto3
import uuid
from datetime import datetime

s3 = boto3.client('s3')

BUCKET_NAME = 'ecommerce-event-data-goutham-01'
FOLDER_NAMES = ['raw-events/', 'processed-events/', 'transformed-events/']

def ensure_folders_exist():
    for folder in FOLDER_NAMES:
        try:
            # Create a placeholder file to ensure the folder (prefix) exists
            s3.put_object(Bucket=BUCKET_NAME, Key=folder + '.keep', Body='')
            print(f"✅ Ensured folder '{folder}' exists in S3")
        except Exception as e:
            print(f"⚠️ Could not ensure folder '{folder}': {e}")

def lambda_handler(event, context):
    ensure_folders_exist()

    # Support both API Gateway and direct invocation
    body = json.loads(event["body"]) if "body" in event else event

    # Add timestamp
    body['event_time'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    # Generate unique filename
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')
    file_id = str(uuid.uuid4())
    key = f"raw-events/{timestamp}_{file_id}.json"

    # Upload to S3
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(body),
            ContentType='application/json'
        )
        print(f"✅ Event stored in S3: {key}")
        return {
            'statusCode': 200,
            'body': json.dumps('✅ Event stored successfully in S3')
        }
    except Exception as e:
        print(f"❌ Failed to store event: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('❌ Error storing event')
        }
