from airflow.decorators import dag, task
from datetime import datetime
from helper.extract import Extract
from helper.transform import Transform
from helper.load import Load
from helper.etl_process import ETLProcess
from helper.config import *



@dag(
    dag_id="ETL",
    start_date=datetime(2021, 10, 26),
    catchup=False,
    schedule_interval="@daily",
)
def this_dag():
    @task()
    def etl_dim_product():
        # Initialize classes for dim_brand ETL
        extract = Extract(api_url=url_template, headers=header, bucket_name=bucket)
        transform = Transform(bucket_name=bucket)
        load = Load(db_url)
        table_name = "dim_product"

        # Run ETL process for dim_brand
        etl = ETLProcess(extract, transform, load)
        extracted_file = etl.run_extract()
        parquet_path = etl.run_transform(dim_product, extracted_file, table_name)
        etl.run_load(table_name, parquet_path)

        # Return the extracted file path for the next task
        return extracted_file

    @task()
    def tl_fact_sales(extracted_file):
        # Initialize classes for fact_sales TL
        transform = Transform(bucket_name=bucket)
        load = Load(db_url)
        table_name = "fact_sales"

        # Run TL process for fact_sales
        etl = ETLProcess(None, transform, load)  # Pass None for extract since it's TL
        parquet_path = etl.run_transform(fact_sales, extracted_file, table_name)
        etl.run_load(table_name, parquet_path)

    # Define task dependencies
    extracted_file = etl_dim_product()
    tl_fact_sales(extracted_file)


this_dag = this_dag()





