import httpx
import pandas as pd
from datetime import datetime
import sys

def remove_keys_recursive(d, keys_to_remove):
    if isinstance(d, dict):
        return {
            key: remove_keys_recursive(value, keys_to_remove)
            for key, value in d.items()
            if key not in keys_to_remove
        }
    elif isinstance(d, list):
        return [remove_keys_recursive(item, keys_to_remove) for item in d]
    else:
        return d

def rename_quantity_sold(product):
    if "quantity_sold" in product:
        quantity_sold = product.pop("quantity_sold")
        product["quantity_sold_value"] = quantity_sold["value"]
    return product

def make_api_request(url, params, headers):
    with httpx.Client() as client:
        response = client.get(url, params=params, headers=headers)
    return response

def flatten_data(item):
    flattened_data = {
        key: value for key, value in item.items() if key not in ["visible_impression_info"]
    }
    
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

def save_to_local(data, timestamp):
    # Remove specified keys from all levels of the nested dictionary
    keys_to_remove = ['impression_info', 'badges_new']
    cleaned_data = remove_keys_recursive(data, keys_to_remove)

    # Rename the "quantity_sold" field to "quantity_sold_value"
    cleaned_data = [rename_quantity_sold(product) for product in cleaned_data]

    # Flatten the nested structure
    cleaned_data = [flatten_data(product) for product in cleaned_data]

    # Convert the cleaned data to a Pandas DataFrame
    pd_df = pd.DataFrame(cleaned_data)

    # Convert timestamp to Unix time
    unix_timestamp = int(datetime.timestamp(timestamp))

    pd_df["ingestion_dt_unix"] = unix_timestamp
    pd_df.rename(columns={"id": "tiki_pid"}, inplace=True)
    pd_df["ingestion_date"] = pd.to_datetime(pd.to_datetime(pd_df["ingestion_dt_unix"], unit='s').dt.date)

    pd_df["quantity_sold_value"] = pd_df["quantity_sold_value"] + 4

    ## Specify local path for saving
    local_path_csv = f'raw_116532_{unix_timestamp}.csv'
    ## Write the DataFrame to local CSV file
    pd_df.to_csv(local_path_csv, index=False)

    return pd_df
