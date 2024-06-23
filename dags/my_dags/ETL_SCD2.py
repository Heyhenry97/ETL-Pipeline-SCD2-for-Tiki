from airflow.decorators import dag, task
from datetime import datetime
from helper.extract_SCD2 import Extract
from helper.transform import Transform
from helper.load_SCD2 import Load
from helper.etl_process import ETLProcess
from helper.config import *

# Define the DAG using the @dag decorator
@dag(
    dag_id="ETL_SCD2",
    start_date=datetime(2021, 10, 26),
    catchup=False,
    schedule_interval="@daily",
)
def etl_scd2_dag():
    @task()
    def etl_staging_dim_product():
        # Initialize classes for dim_product ETL
        extract = Extract(api_url=url_template, headers=header, bucket_name=bucket)
        transform = Transform(bucket_name=bucket)
        load = Load(db_url)
        table_name = "staging_dim_product"

        # Run ETL process for dim_product
        etl = ETLProcess(extract, transform, load)
        extracted_file = etl.run_extract()
        parquet_path = etl.run_transform(dim_product, extracted_file, table_name)
        etl.run_load(table_name, parquet_path)

        # Return the extracted file path for the next task (if needed)
        return extracted_file

    # Define task dependencies
    etl_dim_product_task = etl_staging_dim_product()

etl_scd2_dag = etl_scd2_dag()
