import os
import sys
import pandas as pd
module_path = os.path.abspath(os.path.join('..'))

if module_path not in sys.path:
    sys.path.append(module_path)

from helper.util_minio import MinioHandler  # MinioHandler is a class in util_minio module
from helper import config

class DataTransformer:
    def __init__(self, bucket_name, file_path):
        self.bucket_name = bucket_name
        self.file_path = file_path
        self.dict_storage_options = config.MINIO_CONFIG
    
    def download_to_dataframe(self):
        minio_handle = MinioHandler()  # Assuming MinioHandler is properly implemented
        return minio_handle.download_to_dataframe(self.bucket_name, self.file_path)

    def save_to_parquet(self, df, partition_cols, cols_dim):
        df_dim_product = df[cols_dim]
        df_dim_product.to_parquet('s3://tiki/curated/dim_product', 
                                  storage_options=self.dict_storage_options, 
                                  partition_cols=partition_cols,
                                  ) #TODO: add append to false

# Define input parameters
