import pandas as pd
import os
import boto3
import pytest
from datetime import datetime
from unittest.mock import patch
import traceback


boto3.setup_default_session(region_name="eu-west-1", profile_name="eprod")


envi_txt = """
secret_name\taffise_mysql_rdsMySQL
bucket_name\tpublic-filesharing-e37f4xw
database\tadstart_dashboard
"""


envi_local = dict(item.split("\t") for item in envi_txt.strip().split("\n"))

# print(os.environ)
with patch.dict(os.environ, envi_local):
    import lambda_handler

if __name__ == "__main__":
    start_time = datetime.now()
    try:
        # event = {"date": "2024-06-05"}
        event = {}
        results = lambda_handler.lambda_handler(event, 0)
        # print(results["query"])
    except Exception as e:
        print("error: ")
        print(e)
        traceback.print_exc()
    finally:
        # Measure the end time
        end_time = datetime.now()

        # Calculate the runtime
        runtime = end_time - start_time
        print("Rung time: ", runtime)
