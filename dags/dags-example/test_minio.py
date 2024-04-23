from airflow.decorators import dag, task
from datetime import datetime
from helper import util_minio

import os
import time

@dag(dag_id='test_minio',
     start_date=datetime(2021, 10, 26),
     catchup=False,
     schedule_interval='@daily')
def dag_setting():
    @task()
    def download_file():
        
        minio_handler = util_minio.MinioHandler()
        df = minio_handler.download_to_dataframe('tiki', 'raw_116532_1710396371.csv')
        print(df)


    download_file()

greet_dag = dag_setting()

