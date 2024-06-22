from sqlalchemy import inspect
import os, sys
import pandas as pd
from sqlalchemy import create_engine
from helper.config import incremental_product

module_path = os.path.abspath(os.path.join(".."))
if module_path not in sys.path:
    sys.path.append(module_path + "/my_utils")

from helper.util_minio import MinioHandler

class Load:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        self.update_incremental_fact_sales = incremental_product
        
    def update_fact_sales(self):
        with self.engine.connect() as conn:
            conn.execute(self.update_incremental_fact_sales)

    def execute(self, parquet_path, table_name):
        minio_handler = MinioHandler()
        df = pd.read_parquet(
            parquet_path, storage_options=minio_handler.storage_options
        )

        inspector = inspect(self.engine)

        if table_name == "fact_sales":
            df.to_sql(table_name, self.engine, if_exists="append", index=False)
            self.update_fact_sales()
        else:
            # For dimension tables, only add data if the table doesn't already exist
            if not inspector.has_table(table_name):
                df.to_sql(table_name, self.engine, if_exists="append", index=True)
            else:
                print(f"Table {table_name} already exists, skipping load.")


# from sqlalchemy import inspect
# import os, sys
# import pandas as pd
# from sqlalchemy import create_engine
# from helper.config import dim_product_table, fact_sales_table, incremental_product


# module_path = os.path.abspath(os.path.join(".."))
# if module_path not in sys.path:
#     sys.path.append(module_path + "/my_utils")

# from helper.util_minio import MinioHandler


# class Load:
#     def __init__(self, db_url):
#         self.engine = create_engine(db_url)
#         self.create_dim_product_table = dim_product_table
#         self.create_fact_sales = fact_sales_table
#         self.incremental_update_sql = incremental_product

#     def create_table1(self):
#         with self.engine.connect() as conn:
#             conn.execute(self.create_dim_product_table)

#     def create_table2(self):
#         with self.engine.connect() as conn:
#             conn.execute(self.create_fact_sales)
            
#     def incremental_update(self):
#         with self.engine.connect() as conn:
#             conn.execute(self.incremental_update_sql)

    # def execute(self, parquet_path, table_name):
    #     minio_handler = MinioHandler()
    #     df = pd.read_parquet(
    #         parquet_path, storage_options=minio_handler.storage_options
    #     )

    #     inspector = inspect(self.engine)

    #     # Ensure tables are created before any data loading
    #     self.create_table1()
    #     self.create_table2()
    #     if table_name == "fact_sales":
    #         df.to_sql(table_name, self.engine, if_exists="append", index=False)
    #         # self.incremental_update()
    #     else:
    #         if not inspector.has_table(table_name):
    #             df.to_sql(table_name, self.engine, if_exists="append", index=False)
    #         else:
    #             print(f"Table {table_name} already exists, skipping load.")

    # def execute(self, parquet_path, table_name):
    #     minio_handler = MinioHandler()
    #     df = pd.read_parquet(
    #         parquet_path, storage_options=minio_handler.storage_options
    #     )
    #     self.create_table1()
    #     self.create_table2()
    #     df.to_sql(table_name, self.engine, if_exists="append", index=False)
    #     self.incremental_update()

    
