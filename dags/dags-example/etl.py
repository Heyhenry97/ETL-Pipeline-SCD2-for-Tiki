from airflow.decorators import dag, task
from datetime import datetime
from helper import util_minio

from helper.module_1_extract import  make_api_request, save_to_local
from helper.module_2_transform import DataTransformer
from helper.module_3_load import DataLoader

from helper import config

import os
import time

@dag(dag_id='etl',
     start_date=datetime(2021, 10, 26),
     catchup=False,
     schedule_interval='@daily')
def dag_setting():
    @task()
    def extract():
        
        # API request
        url = 'https://api.tiki.vn/seller-store/v2/collections/116532/products'
        params = {'limit': 100, 'cursor': 40}
        headers = {'x-source': 'local', 'Host': 'api.tiki.vn'}

        response = make_api_request(url, params, headers)

        if response.status_code == 200:
            data = response.json()['data']

            timestamp = datetime.now()

            # Save to local files
            cleaned_data_out = save_to_local(data, timestamp)

        else:
            print(f"Error: {response.status_code}")
            print(response.text)

        # Display the Pandas DataFrame
        cleaned_data_out.head(2)

        minio_handler = util_minio.MinioHandler()
        minio_handler.save_dataframe_to_csv("tiki", "raw/raw_116532_1713882043.csv", cleaned_data_out)

    @task()
    def transform():
        print('transform')


        # Define input parameters
        bucket_name = config.BUCKET_CONFIG
        file_path = 'raw/raw_116532_1713882043.csv'

        partition_cols = ['ingestion_date']
        cols_dim = ["tiki_pid","name","brand_name","origin",'ingestion_date','ingestion_dt_unix']  # Define the columns here

        # Create instance of DataHandler
        data_handler = DataTransformer(bucket_name, file_path)

        # Download data to DataFrame
        df = data_handler.download_to_dataframe()

        # Save DataFrame to Parquet
        data_handler.save_to_parquet(df, partition_cols=partition_cols, cols_dim=cols_dim)

    @task()
    def load():
        print('load')

        parquet_path = 's3://tiki/curated/dim_product/ingestion_date=2024-04-23'
        postgres_username = config.DATABASE_CONFIG['username']
        postgres_password = config.DATABASE_CONFIG['password']
        postgres_host = config.DATABASE_CONFIG['host']
        postgres_port = config.DATABASE_CONFIG['port']
        postgres_db = config.DATABASE_CONFIG['db_name']

        table_name = 'dim_product'

        # Create instance of DataLoader
        data_processor = DataLoader()

        # Read data from Parquet file
        parquet_df = data_processor.read_parquet(parquet_path)

        # Find the row with the maximum 'ingestion_dt_unix'
        latest_row = parquet_df.loc[parquet_df['ingestion_dt_unix'].idxmax()]

        # Filter DataFrame to get the latest data
        latest_parquet = parquet_df[parquet_df['ingestion_dt_unix'] == latest_row['ingestion_dt_unix']]

        # Define PostgreSQL connection string
        connection_string = f'postgresql://{postgres_username}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}'

        # Write the latest data to PostgreSQL
        data_processor.write_to_postgres(latest_parquet, table_name, connection_string)

        # Display the latest data
        print(latest_parquet)

    extract()  >>  transform() >> load()


greet_dag = dag_setting()

