import os
import httpx
import pandas as pd
from datetime import datetime
import sys
import time
from helper.config import tiki_pid_list_staging

module_path = os.path.abspath(os.path.join(".."))
if module_path not in sys.path:
    sys.path.append(module_path + "/my_utils")

from helper.util_minio import MinioHandler


class Extract:
    def __init__(self, api_url, headers, bucket_name):
        self.api_url = api_url
        self.headers = headers
        self.minio_handler = MinioHandler()
        self.bucket_name = bucket_name
        self.all_data = []  # List to store data from all pages

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

    def make_api_request(self, cursor):
        url = self.api_url.format(cursor=cursor)
        with httpx.Client() as client:
            response = client.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()["data"]

    def flatten_data(self, item):
        flattened_data = {
            key: value
            for key, value in item.items()
            if key not in ["visible_impression_info"]
        }
        visible_impression_info = item.get("visible_impression_info", {}).get(
            "amplitude", {}
        )
        fields_to_extract = [
            "category_l1_name",
            "category_l2_name",
            "category_l3_name",
            "seller_type",
            "primary_category_name",
            "is_imported",
        ]
        flattened_data.update(
            {
                field: visible_impression_info.get(field, None)
                for field in fields_to_extract
            }
        )
        return flattened_data

    def save_to_df(self, data, unix_timestamp):
        keys_to_remove = ["impression_info", "badges_new"]
        cleaned_data = self.remove_keys_recursive(data, keys_to_remove)
        cleaned_data = [self.rename_quantity_sold(product) for product in cleaned_data]
        cleaned_data = [self.flatten_data(product) for product in cleaned_data]
        pd_df = pd.DataFrame(cleaned_data)
        pd_df["ingestion_dt_unix"] = (
            unix_timestamp  # Use the same timestamp for all records
        )
        pd_df.rename(columns={"id": "tiki_pid"}, inplace=True)
        pd_df["ingestion_date"] = pd.to_datetime(
            pd.to_datetime(pd_df["ingestion_dt_unix"], unit="s").dt.date
        )
        pd_df["valid_from"] = pd_df["ingestion_dt_unix"]
        pd_df["valid_to"] = None
        pd_df["is_active"] = True
        pd_df["quantity_sold_value"] = pd_df["quantity_sold_value"] + 4
        # pd_df.rename(columns={"master_product_sku":"product_id"},inplace=True)
        return pd_df

    def save_to_minio(self, dataframe, file_name):
        self.minio_handler.save_dataframe_to_csv(self.bucket_name, file_name, dataframe)

    def concatenate_and_save(self):
        concatenated_df = pd.concat(self.all_data, ignore_index=True)
        concatenated_df["incremental_product"] = range(1, len(concatenated_df) + 1)
        concatenated_df= concatenated_df[concatenated_df['tiki_pid'].isin(tiki_pid_list_staging)==True]
        concatenated_df.loc[concatenated_df["tiki_pid"] == 74021317, 'primary_category_name'] = "Kinh Dị"
        concatenated_df.loc[concatenated_df["tiki_pid"] == 188940817, 'primary_category_name'] = "Trinh Thám"
        timestamp = datetime.now()
        unix_timestamp = int(datetime.timestamp(timestamp))
        csv_name = f"{unix_timestamp}.csv"
        self.save_to_minio(concatenated_df, f"raw/{csv_name}")
        print(f"Exported concatenated CSV to Minio: {csv_name}")
        return csv_name

    def execute(self):
        timestamp = datetime.now()
        unix_timestamp = int(datetime.timestamp(timestamp))  # Set the timestamp once

        for i in range(0, 400, 40):
            data = self.make_api_request(cursor=i)
            df = self.save_to_df(
                data, unix_timestamp
            )  # Pass the timestamp to save_to_df
            self.all_data.append(df)

        csv_name = self.concatenate_and_save()
        return csv_name
