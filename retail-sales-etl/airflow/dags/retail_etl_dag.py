from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

def load_raw_data():
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/retail')
    df = pd.read_csv('/opt/airflow/data/raw_sales.csv')
    df.to_sql('sales', engine, schema='raw', if_exists='replace', index=False)

def create_schema():
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/retail')
    with engine.connect() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

with DAG(
    dag_id='retail_sales_etl',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    init_schema = PythonOperator(
        task_id='create_schema',
        python_callable=create_schema
    )

    load_data = PythonOperator(
        task_id='load_raw_sales',
        python_callable=load_raw_data
    )

    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt/retail_dbt_project && dbt run'
    )

    init_schema >> load_data >> run_dbt