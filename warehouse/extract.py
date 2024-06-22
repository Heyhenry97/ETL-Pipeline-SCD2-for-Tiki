import os
import httpx
import pandas as pd
from datetime import datetime
import sys

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path + "/my_utils")

from helper.util_minio import MinioHandler

class Extract:
    def __init__(self, api_url, params, headers, bucket_name):
        self.api_url = api_url
        self.params = params
        self.headers = headers
        self.minio_handler = MinioHandler()
        self.bucket_name = bucket_name

    def remove_keys_recursive(self, d, keys_to_remove):
        if isinstance(d, dict):
            return {
                key: self.remove_keys_recursive(value, keys_to_remove)
                for key, value in d.items()
                if key not in keys_to_remove
            }
        elif isinstance(d, list):
            return [self.remove_keys_recursive(item, keys_to_remove) for item in d]
        else:
            return d

    def rename_quantity_sold(self, product):
        if "quantity_sold" in product:
            quantity_sold = product.pop("quantity_sold")
            product["quantity_sold_value"] = quantity_sold["value"]
        return product

    def make_api_request(self):
        with httpx.Client() as client:
            response = client.get(self.api_url, params=self.params, headers=self.headers)
        response.raise_for_status()
        return response.json()['data']

    def flatten_data(self, item):
        flattened_data = {key: value for key, value in item.items() if key not in ["visible_impression_info"]}
        visible_impression_info = item.get("visible_impression_info", {}).get("amplitude", {})
        fields_to_extract = [
            "category_l1_name",
            "category_l2_name",
            "category_l3_name",
            "seller_type",
            "primary_category_name",
            "is_imported",
        ]
        flattened_data.update({field: visible_impression_info.get(field, None) for field in fields_to_extract})
        return flattened_data

    def save_to_df(self, data):
        keys_to_remove = ['impression_info', 'badges_new']
        cleaned_data = self.remove_keys_recursive(data, keys_to_remove)
        cleaned_data = [self.rename_quantity_sold(product) for product in cleaned_data]
        cleaned_data = [self.flatten_data(product) for product in cleaned_data]
        pd_df = pd.DataFrame(cleaned_data)
        timestamp = datetime.now()
        unix_timestamp = int(datetime.timestamp(timestamp))
        pd_df["ingestion_dt_unix"] = unix_timestamp
        pd_df.rename(columns={"id": "tiki_pid"}, inplace=True)
        pd_df["ingestion_date"] = pd.to_datetime(pd.to_datetime(pd_df["ingestion_dt_unix"], unit='s').dt.date)
        pd_df["quantity_sold_value"] = pd_df["quantity_sold_value"] + 4
        csv_name = f'raw_{unix_timestamp}.csv'
        return pd_df, csv_name

    def save_to_minio(self, dataframe, file_name):
        self.minio_handler.save_dataframe_to_csv(self.bucket_name, file_name, dataframe)

    def execute(self):
        data = self.make_api_request()
        df, csv_name = self.save_to_df(data)
        self.save_to_minio(df, f"raw/{csv_name}")
        print(f'Exported to Minio: {csv_name}')
        return csv_name