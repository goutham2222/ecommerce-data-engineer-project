import json
import urllib3

http = urllib3.PoolManager()

def lambda_handler(event, context):
    # Fetch products from DummyJSON
    response = http.request('GET', 'https://dummyjson.com/products')
    
    if response.status != 200:
        return {
            "statusCode": response.status,
            "body": json.dumps({"message": "Failed to fetch data from DummyJSON"})
        }

    products = json.loads(response.data.decode('utf-8')).get("products", [])

    # Send all products in a single POST request to ingestion API
    ingestion_url = "https://ca3u8lra36.execute-api.us-west-2.amazonaws.com/prod/ingest"

    ingestion_response = http.request(
        'POST',
        ingestion_url,
        body=json.dumps(products),
        headers={'Content-Type': 'application/json'}
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"{len(products)} products sent in one request.",
            "apiResponse": ingestion_response.status
        })
    }
