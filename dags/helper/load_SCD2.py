import os, sys
import pandas as pd
from sqlalchemy import create_engine
from helper.config import (
    staging_dim_product_table,
    insert_change_record_dim_product,
    update_change_record_dim_product,
    insert_new_record_dim_product,
    incremental_product,
)

module_path = os.path.abspath(os.path.join(".."))
if module_path not in sys.path:
    sys.path.append(module_path + "/my_utils")

from helper.util_minio import MinioHandler


class Load:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        self.create_staging_dim_product = staging_dim_product_table
        self.truncate_staging_dim_product = """ TRUNCATE TABLE staging_dim_product; """
        self.insert_change_dim_product = insert_change_record_dim_product
        self.insert_new_dim_product = insert_new_record_dim_product
        self.update_change_dim_product = update_change_record_dim_product
        self.update_incremental_fact_sales = incremental_product

    def create_table(self):
        with self.engine.connect() as conn:
            conn.execute(self.create_staging_dim_product)

    def truncate_table(self):
        with self.engine.connect() as conn:
            conn.execute(self.truncate_staging_dim_product)

    def new_record(self):
        with self.engine.connect() as conn:
            conn.execute(self.insert_new_dim_product)

    def insert_table(self):
        with self.engine.connect() as conn:
            conn.execute(self.insert_change_dim_product)

    def update_dim_product(self):
        with self.engine.connect() as conn:
            conn.execute(self.update_change_dim_product)

    def update_fact_sales(self):
        with self.engine.connect() as conn:
            conn.execute(self.update_incremental_fact_sales)

    def execute(self, parquet_path, table_name):

        minio_handler = MinioHandler()
        df = pd.read_parquet(
            parquet_path, storage_options=minio_handler.storage_options
        )
        self.create_table()
        self.truncate_table()
        df.to_sql(table_name, self.engine, if_exists="append", index=False)
        self.insert_table()
        self.update_dim_product()
        self.new_record()
        self.update_fact_sales()
        
        

