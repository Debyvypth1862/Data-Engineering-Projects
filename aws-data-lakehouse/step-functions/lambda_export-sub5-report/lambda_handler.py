import os
import boto3
import time
from datetime import datetime, timezone

# Initialize the Athena client outside the handler for better performance
athena_client = boto3.client("athena")

# Define the ordered columns
ORDERED_COLUMNS = [
    "datestamp",
    "affiliate_id",
    "affiliate_name",
    "affiliate_manager",
    "advertiser_name",
    "account_manager",
    "offer_title",
    "offer_id",
    "country",
    "sub5",
    "sub6",
    "click_count",
    "host_count",
    "approved_revenue",
    "conversions_count"
]

def execute_athena_query(query):
    """Execute Athena query and wait for completion"""
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": "affise_data_fusion"},
            ResultConfiguration={
                "OutputLocation": "s3://adstart.database/athena_results/"
            },
        )

        query_execution_id = response["QueryExecutionId"]

        while True:
            query_status = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            state = query_status["QueryExecution"]["Status"]["State"]

            if state == "SUCCEEDED":
                print("Query completed successfully")
                break
            elif state in ["FAILED", "CANCELLED"]:
                error_message = query_status["QueryExecution"]["Status"].get(
                    "StateChangeReason", "No error message"
                )
                raise Exception(f"Query failed: {error_message}")

            time.sleep(1)

        return query_execution_id
    except Exception as e:
        print(f"Error executing Athena query: {e}")
        raise

def get_all_query_results(execution_id):
    """Retrieve all results from Athena using pagination"""
    try:
        all_rows = []
        next_token = None
        first_page = True
        columns = None

        while True:
            # Get results page
            if next_token:
                results = athena_client.get_query_results(
                    QueryExecutionId=execution_id,
                    NextToken=next_token
                )
            else:
                results = athena_client.get_query_results(
                    QueryExecutionId=execution_id
                )

            # Process the current page
            current_rows = results['ResultSet']['Rows']

            # Handle the first page (contains headers)
            if first_page:
                header_row = current_rows[0]
                columns = [field['VarCharValue'] for field in header_row['Data']]
                current_rows = current_rows[1:]  # Skip header row
                first_page = False

            # Process rows
            for row in current_rows:
                row_data = {}  # Changed to dict for easier column mapping
                for col, val in zip(columns, row['Data']):
                    value = val.get('VarCharValue', '')
                    if value is None:
                        value = ''
                    row_data[col] = value
                all_rows.append(row_data)

            # Check if there are more results
            next_token = results.get('NextToken')
            if not next_token:
                break

        return columns, all_rows
    except Exception as e:
        print(f"Error retrieving query results: {e}")
        raise

def escape_csv_field(field):
    """Properly escape and quote CSV fields containing special characters"""
    field_str = str(field)
    if any(char in field_str for char in [',', '"', '\n', '\r']):
        # Escape double quotes by doubling them and wrap the field in quotes
        return '"' + field_str.replace('"', '""') + '"'
    return field_str

def format_csv_content(columns, rows):
    """Format the data in the specified column order with proper CSV escaping"""
    # Create CSV header
    csv_content = ','.join(ORDERED_COLUMNS) + '\n'
    
    # Add rows in the correct order with proper escaping
    for row in rows:
        ordered_row = [escape_csv_field(row.get(col, '')) for col in ORDERED_COLUMNS]
        csv_content += ','.join(ordered_row) + '\n'
    
    return csv_content

def lambda_handler(event, context):
    try:
        # Get process date from event or use current date
        process_date = event.get("date")
        current_time = datetime.now(timezone.utc)
        date_string = process_date or current_time.strftime("%Y-%m-%d")
        
        # Format the date for output filename
        process_date_dt = datetime.strptime(date_string, "%Y-%m-%d")
        output_date = f"{process_date_dt.year}{int(process_date_dt.month)}{int(process_date_dt.day)}"
        
        # Define output file paths
        file_name = f"reports/Sub5-{output_date}.csv"
        
        # Execute the query and get results
        print(f"Starting query execution at {current_time}")
        execution_id = execute_athena_query(ATHENA_QUERY)
        
        # Get all results using pagination
        columns, rows = get_all_query_results(execution_id)
        
        # Format CSV content with ordered columns
        csv_content = format_csv_content(columns, rows)
        record_count = len(rows)
        
        print(f"Successfully processed {record_count} records from query results")

        # Upload results to S3
        s3_client = boto3.client("s3")
        response = s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=csv_content
        )
        
        # Check if upload was successful
        success = response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
        if success:
            print(f"Successfully uploaded report to s3://{BUCKET_NAME}/{file_name}")
            return {
                'statusCode': 200,
                'body': {
                    'message': 'Report generated successfully',
                    'recordCount': record_count,
                    'outputPath': f"s3://{BUCKET_NAME}/{file_name}"
                }
            }
        else:
            raise Exception("Failed to upload report to S3")
            
    except Exception as e:
        print(f"Error in lambda handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e)
            }
        }

# Environment variables
BUCKET_NAME = os.environ["bucket_name"]

ATHENA_QUERY = """
SELECT 
    datestamp,
    affiliate_id,
    affiliate_name,
    affiliate_manager,
    advertiser_name,
    account_manager,
    offer_title,
    offer_id,
    country,
    sub5,
    sub6,
    click_count,
    host_count,
    approved_revenue,
    conversions_count
    FROM affise_data_fusion.daily_report_sub5_output 
    WHERE datestamp BETWEEN date_add('day', -7, current_date)
    AND current_date
    AND conversions_count > 0
"""