from datetime import datetime, timezone, timedelta
import requests
import time
import boto3
import json
import base64
from io import StringIO
import os

def get_secret(secret_name):
    global CREDENTIALS
    if CREDENTIALS is not None:
        return CREDENTIALS

    print("Get secret")
    client = boto3.client(service_name="secretsmanager")
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    if "SecretString" in get_secret_value_response:
        secret = get_secret_value_response["SecretString"]
    else:
        secret = base64.b64decode(get_secret_value_response["SecretBinary"])

    CREDENTIALS = json.loads(secret)
    return CREDENTIALS

def call_affise_api(url, params, headers):
    for i in range(5):
        try:
            results = requests.get(url=url, params=params, headers=headers)
            if results.status_code != 200:
                print(f"API call failed with status code: {results.status_code}")
                print(f"Response: {results.text}")
                raise Exception(f"API call failed with status code: {results.status_code}")
            return results
        except Exception as e:
            print(f"Error calling API: {str(e)}")
            time.sleep(2 * i)
            print("Retry call Affise API")

    raise Exception("Fail call Affise API after 5 times")

def delete_objects_recursive(bucket_name, prefix=""):
    print(f"Delete all object at bucket: {bucket_name}, prefix: {prefix}")
    
    paginator = S3.get_paginator("list_objects_v2")
    
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            delete_keys = [{"Key": obj["Key"]} for obj in page["Contents"]]
            
            response = S3.delete_objects(
                Bucket=bucket_name,
                Delete={
                    "Objects": delete_keys,
                }
            )
            
            print(f"Deleted {len(delete_keys)} objects")
            print("Deleted objects: ", response["Deleted"])

def flatten_landing_pages(offers):
    flattened_offers = []
    for offer in offers:
        offer_copy = offer.copy()
        # Include landing information if available
        if offer.get("landings"):
            landing = offer["landings"][0]
            offer_copy.update({
                "landing_url_preview": landing.get("url_preview", ""),
                "landing_id": landing.get("id", ""),
                "landing_title": landing.get("title", ""),
                "landing_type": landing.get("type", ""),
                "landing_url": landing.get("url", "")
            })
        else:
            # Add empty landing fields for offers without landings
            offer_copy.update({
                "landing_url_preview": "",
                "landing_id": "",
                "landing_title": "",
                "landing_type": "",
                "landing_url": ""
            })
        flattened_offers.append(offer_copy)
    return flattened_offers

def process_offers():
    api_key = get_secret(SECRET_NAME)["affiseKey"]
    headers = {"API-Key": api_key}
    url = "http://api-adstartmedia.affise.com/3.0/offers"
    
    params = {
        "status[]": ["active", "stopped", "suspended"],
        "limit": 500,
        "page": 1
    }
    
    folder_prefix = f"{FOLDER_PREFIX}/full_offers/"
    
    # Clear existing files
    delete_objects_recursive(BUCKET_NAME, folder_prefix)
    
    # Create a StringIO object to store all offers
    all_offers_buffer = StringIO()
    total_offers = 0
    total_processed = 0
    
    while params["page"] is not None:
        results = call_affise_api(url, params, headers)
        print(f"Processing page {params['page']}")
        print(f"API URL: {results.url}")
        
        results_json = results.json()
        current_offers = results_json["offers"]
        print(f"Raw offers in current batch: {len(current_offers)}")
        total_offers += len(current_offers)
        
        # Process offers
        processed_offers = flatten_landing_pages(current_offers)
        print(f"Processed offers in current batch: {len(processed_offers)}")
        
        if processed_offers:
            # Append processed offers to the combined buffer
            for offer in processed_offers:
                json.dump(offer, all_offers_buffer)
                all_offers_buffer.write("\n")
                total_processed += 1
        
        params["page"] = results_json["pagination"].get("next_page")
        print(f"Next page: {params['page']}")
        print("-" * 50)
    
    # Save the combined file
    combined_file_key = f"{folder_prefix}combined_offers.json"
    S3.put_object(
        Body=all_offers_buffer.getvalue(),
        Bucket=BUCKET_NAME,
        Key=combined_file_key,
    )
    
    print(f"Total raw offers retrieved: {total_offers}")
    print(f"Total offers processed and saved: {total_processed}")
    print(f"Combined file saved to: {combined_file_key}")

def lambda_handler(event, context):
    print("Event: ", event)
    process_offers()
    return {
        'statusCode': 200,
        'body': json.dumps('Offers processing completed successfully')
    }

# Global variables
FOLDER_PREFIX = os.environ["folder_prefix"]
SECRET_NAME = os.environ["secret_name"]
BUCKET_NAME = os.environ["bucket_name"]
CREDENTIALS = None
S3 = boto3.client("s3")