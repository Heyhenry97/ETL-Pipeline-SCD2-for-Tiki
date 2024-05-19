from airflow.decorators import dag, task
from datetime import datetime

from helper.extract import Extract
from helper.transform import Transform
from helper.load import Load
from helper.etl_process import ETLProcess
import os
import time


api_url = 'https://api.tiki.vn/seller-store/v2/collections/116532/products'
params = {'limit': 100, 'cursor': 40}
headers = {'x-source': 'local', 'Host': 'api.tiki.vn'}
bucket_name = "tiki"
db_url = 'postgresql://my_user:my_password@pg-tiki:5432/dw_tiki'
transform_columns = ["tiki_pid", "name", "brand_name", "origin", 'ingestion_date', 'ingestion_dt_unix']
table_name = 'dim_product'



@dag(dag_id='etl_product',
     start_date=datetime(2021, 10, 26),
     catchup=False,
     schedule_interval='@daily')
def this_dag():
    @task()
    def task_etl_product():
        # Initialize classes
        extract = Extract(api_url, params, headers, bucket_name)
        transform = Transform(bucket_name)
        load = Load(db_url)

        # Run ETL process
        etl = ETLProcess(extract, transform, load)
        etl.run(transform_columns, table_name)

    task_etl_product()


this_dag = this_dag()

