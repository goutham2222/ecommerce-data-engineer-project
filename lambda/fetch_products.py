import json
import urllib3

http = urllib3.PoolManager()

def lambda_handler(event, context):
    response = http.request('GET', 'https://dummyjson.com/products', timeout=5.0)

    if response.status != 200:
        return {
            "statusCode": response.status,
            "body": json.dumps({"message": "Failed to fetch data from DummyJSON"})
        }

    products = json.loads(response.data.decode('utf-8')).get("products", [])

    ingestion_url = "https://ca3u8lra36.execute-api.us-west-2.amazonaws.com/prod/ingest"

    response = http.request(
        'POST',
        ingestion_url,
        body=json.dumps(products),
        headers={'Content-Type': 'application/json'},
        timeout=10.0
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"{len(products)} products sent in one request.",
            "apiResponse": response.status
        })
    }
