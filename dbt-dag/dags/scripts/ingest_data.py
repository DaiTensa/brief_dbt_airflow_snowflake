
import os
import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Snowflake connection details from Environment Variables
SF_ACCOUNT = os.getenv('SF_ACCOUNT')
SF_USER = os.getenv('SF_USER')
SF_PASSWORD = os.getenv('SF_PASSWORD')
SF_ROLE = os.getenv('SF_ROLE')
SF_WAREHOUSE = os.getenv('SF_WAREHOUSE')
SF_DATABASE = os.getenv('SF_DATABASE')
SF_SCHEMA = os.getenv('SF_SCHEMA')

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"

def get_snowflake_conn():
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        role=SF_ROLE,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA
    )

def download_file(year, month):
    url = BASE_URL.format(year=year, month=month)
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    local_path = os.path.join("/tmp", filename)
    
    logger.info(f"Downloading {url}...")
    response = requests.get(url)
    
    if response.status_code == 200:
        with open(local_path, 'wb') as f:
            f.write(response.content)
        logger.info(f"Downloaded to {local_path}")
        return local_path
    else:
        logger.warning(f"File not found or error: {url} - Status: {response.status_code}")
        return None

def ingest_data(years=[2024]):
    conn = get_snowflake_conn()
    cur = conn.cursor()
    
    # Ensure usage of correct schema
    cur.execute(f"USE DATABASE {SF_DATABASE}")
    cur.execute(f"USE SCHEMA {SF_SCHEMA}")
    
    for year in years:
        for month in range(1, 3):  # Only January and February for testing
                
            local_file = download_file(year, month)
            if local_file:
                try:
                    df = pd.read_parquet(local_file)
                    
                    # Basic standardization (columns to uppercase for Snowflake standard)
                    df.columns = [c.upper() for c in df.columns]
                    
                    logger.info(f"Uploading data for {year}-{month:02d} to Snowflake table YELLOW_TAXI_TRIPS...")
                    
                    success, n_chunks, n_rows, _ = write_pandas(
                        conn,
                        df,
                        "YELLOW_TAXI_TRIPS",
                        auto_create_table=True,
                        chunk_size=100000
                    )
                    
                    logger.info(f"Success: {success}, Chunks: {n_chunks}, Rows: {n_rows}")
                    
                except Exception as e:
                    logger.error(f"Failed to ingest {local_file}: {e}")
                finally:
                    if os.path.exists(local_file):
                        os.remove(local_file)
                        logger.info(f"Removed temp file {local_file}")
    
    conn.close()

if __name__ == "__main__":
    # Check credentials
    if not all([SF_ACCOUNT, SF_USER, SF_PASSWORD]):
        logger.error("Snowflake credentials (SF_ACCOUNT, SF_USER, SF_PASSWORD) must be set in environment variables.")
        exit(1)
        
    ingest_data()
