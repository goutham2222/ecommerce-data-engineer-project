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
            s3.put_object(Bucket=BUCKET_NAME, Key=folder + '.keep', Body='')
        except:
            pass  # Ignore if folder already exists or fails silently

def lambda_handler(event, context):
    ensure_folders_exist()

    try:
        body = json.loads(event["body"]) if "body" in event else event
        events = body if isinstance(body, list) else [body]
    except:
        return {
            'statusCode': 400,
            'body': json.dumps("Invalid request body")
        }

    for item in events:
        try:
            item['event_time'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')
            file_id = str(uuid.uuid4())
            key = f"raw-events/{timestamp}_{file_id}.json"

            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=key,
                Body=json.dumps(item),
                ContentType='application/json'
            )
        except:
            continue  # Skip on failure but continue with others

    return {
        'statusCode': 200,
        'body': json.dumps(f"{len(events)} event(s) stored successfully.")
    }
