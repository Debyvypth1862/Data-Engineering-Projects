import sys
import json
import boto3
import requests
import time
from io import StringIO
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name', 'folder_prefix', 'secret_name'])

## Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define global variables
BUCKET_NAME = args['bucket_name']
FOLDER_PREFIX = args['folder_prefix']
SECRET_NAME = args['secret_name']
S3 = boto3.client('s3')
CREDENTIALS = None

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

def extract_caps_data(offers):
    """
    Extract caps data from offers and format with 'caps_' prefix on each field
    """
    caps_data_list = []
    
    for offer in offers:
        if 'caps' not in offer or not offer['caps']:
            continue
            
        offer_id = offer['offer_id']
        
        for cap in offer['caps']:
            # Create a dictionary with caps_ prefix for each field
            caps_entry = {
                'offer_id': offer_id,
                'caps_period': cap.get('period', ''),
                'caps_type': cap.get('type', ''),
                'caps_value': cap.get('value', ''),
                'caps_goal_type': cap.get('goal_type', ''),
                'caps_affiliate_type': cap.get('affiliate_type', ''),
                'caps_country_type': cap.get('country_type', ''),
            }
            
            # Handle goals which may be a dictionary
            if 'goals' in cap and cap['goals']:
                if isinstance(cap['goals'], dict):
                    goals_str = ', '.join([f"{k}:{v}" for k, v in cap['goals'].items()])
                    caps_entry['caps_goals'] = goals_str
                else:
                    caps_entry['caps_goals'] = str(cap['goals'])
            else:
                caps_entry['caps_goals'] = ''
                
            # Handle affiliates which is an array
            if 'affiliates' in cap and cap['affiliates']:
                caps_entry['caps_affiliates'] = ', '.join(map(str, cap['affiliates']))
            else:
                caps_entry['caps_affiliates'] = ''
                
            # Handle country (which could be null)
            caps_entry['caps_country'] = cap.get('country', '') or ''
            
            caps_data_list.append(caps_entry)
            
    return caps_data_list

def process_caps():
    api_key = get_secret(SECRET_NAME)["affiseKey"]
    headers = {"API-Key": api_key}
    url = "http://api-adstartmedia.affise.com/3.0/offers"
    
    params = {
        "status[]": ["active", "stopped", "suspended"],
        "limit": 500,
        "page": 1
    }
    
    folder_prefix = f"{FOLDER_PREFIX}/caps_data/"
    
    # Clear existing files
    # delete_objects_recursive(BUCKET_NAME, folder_prefix)
    
    # Create a StringIO object to store all caps data
    all_caps_buffer = StringIO()
    total_offers = 0
    total_caps_entries = 0
    
    # Process all pages
    while params["page"] is not None:
        results = call_affise_api(url, params, headers)
        print(f"Processing page {params['page']}")
        print(f"API URL: {results.url}")
        
        results_json = results.json()
        current_offers = results_json["offers"]
        print(f"Raw offers in current batch: {len(current_offers)}")
        total_offers += len(current_offers)
        
        # Extract caps data
        caps_entries = extract_caps_data(current_offers)
        print(f"Caps entries in current batch: {len(caps_entries)}")
        
        if caps_entries:
            # Append caps entries to the combined buffer
            for entry in caps_entries:
                json.dump(entry, all_caps_buffer)
                all_caps_buffer.write("\n")
                total_caps_entries += 1
        
        params["page"] = results_json["pagination"].get("next_page")
        print(f"Next page: {params['page']}")
        print("-" * 50)
    
    # Save the combined file
    combined_file_key = f"{folder_prefix}combined_caps.json"
    S3.put_object(
        Body=all_caps_buffer.getvalue(),
        Bucket=BUCKET_NAME,
        Key=combined_file_key,
    )
    
    print(f"Total offers processed: {total_offers}")
    print(f"Total caps entries extracted: {total_caps_entries}")
    print(f"Combined caps file saved to: {combined_file_key}")

# Main execution
process_caps()
job.commit()