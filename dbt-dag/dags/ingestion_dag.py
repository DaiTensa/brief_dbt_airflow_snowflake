
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
import os
from datetime import timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_snowflake_env():
    """Referencing the connection defined in Airflow to pass as Env Vars"""
    try:
        conn = BaseHook.get_connection("snowflake_taxi_conn") # Make sure this matches the ID in airflow_settings.yaml
        return {
            "SF_ACCOUNT": conn.extra_dejson.get("account") or conn.host, # logic depends on how it's stored
            "SF_USER": conn.login,
            "SF_PASSWORD": conn.password,
            "SF_ROLE": conn.extra_dejson.get("role"),
            "SF_WAREHOUSE": conn.extra_dejson.get("warehouse"),
            "SF_DATABASE": conn.extra_dejson.get("database"),
            "SF_SCHEMA": conn.schema,
        }
    except Exception as e:
        print(f"Warning: Could not retrieve connection: {e}")
        return {}

with DAG(
    'taxi_ingestion_dag',
    default_args=default_args,
    description='Download and Ingest NYC Taxi Data',
    schedule_interval='@monthly',
    start_date=days_ago(1),
    catchup=False,
    tags=['ingestion', 'snowflake'],
) as dag:

    ingest_task = BashOperator(
        task_id='ingest_taxi_data',
        bash_command='python $AIRFLOW_HOME/dags/scripts/ingest_data.py',
        env={**os.environ, **get_snowflake_env()}, # Merge system env with connection env
    )
