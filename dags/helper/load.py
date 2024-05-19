import os, sys
import pandas as pd
from sqlalchemy import create_engine

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path + "/my_utils")

from helper.util_minio import MinioHandler


class Load:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)


    def execute(self, parquet_path, table_name):
        minio_handler = MinioHandler()
        df = pd.read_parquet(parquet_path, storage_options=minio_handler.storage_options)
        latest_row = df.loc[df['ingestion_dt_unix'].idxmax()]
        df_latest = df[df['ingestion_dt_unix'] == latest_row['ingestion_dt_unix']]
        df_latest.to_sql(table_name, self.engine, if_exists='replace', index=False)