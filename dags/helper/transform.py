import os
import sys
import pandas as pd

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path + "/my_utils")

from helper.util_minio import MinioHandler

class Transform:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def execute(self, csv_name, columns):
        # Load data from MinIO
        minio_handler = MinioHandler()
        df = minio_handler.download_to_dataframe(self.bucket_name, f'raw/{csv_name}')
        
        # Transform data by selecting specific columns
        df_transformed = df[columns]
        
        time_stamp_from_csv = csv_name.split('.')[0].split("_")[-1]
        parquet_name = f'transformed_{time_stamp_from_csv}.parquet'
        
        # Save the transformed DataFrame to a Parquet file in MinIO
        parquet_path = f's3://{self.bucket_name}/curated/dim_product/{parquet_name}'
        df_transformed.to_parquet(parquet_path, storage_options=minio_handler.storage_options)
        
        return parquet_path
