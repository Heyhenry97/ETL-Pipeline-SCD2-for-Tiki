import pandas as pd
from sqlalchemy import create_engine
import os
import sys

module_path = os.path.abspath(os.path.join('..'))

if module_path not in sys.path:
    sys.path.append(module_path)
from helper import config

# USE FOR BOTH DIM AND FACT
class DataLoader:
    def __init__(self):
        self.storage_options = config.MINIO_CONFIG
    
    def read_parquet(self, path):
        return pd.read_parquet(path, storage_options=self.storage_options)
    
    def write_to_postgres(self, df, table_name, connection_string, if_exists='replace'):
        engine = create_engine(connection_string)
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        