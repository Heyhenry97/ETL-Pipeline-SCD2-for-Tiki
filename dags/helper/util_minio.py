import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from io import BytesIO
import boto3
import pyarrow.fs as fs


class MinioHandler:
    def __init__(self):
        endpoint_url = 'http://minio:9000'
        access_key = 'minioadmin'
        secret_key = '12345678'
        self.s3_resource = self._get_s3_resource(endpoint_url, access_key, secret_key)
        self.storage_options = {'endpoint_url': 'http://minio:9000', 'key': 'minioadmin', 'secret': '12345678'}
        

    def _get_s3_resource(self, endpoint_url, access_key, secret_key):
        """
        Configures and returns an S3 resource using boto3 for MinIO access.
        """
        return boto3.resource('s3',
                              endpoint_url=endpoint_url,
                              aws_access_key_id=access_key,
                              aws_secret_access_key=secret_key,
                              config=boto3.session.Config(signature_version='s3v4'))

    def download_to_dataframe(self, bucket_name, object_name):
        """
        Downloads data from the specified S3 bucket and object as a pandas DataFrame.
        """
        try:
            obj = self.s3_resource.Bucket(bucket_name).Object(object_name).get()
            data = obj['Body'].read()
            return pd.read_csv(BytesIO(data))  # Adjust for different file formats
        except Exception as e:
            raise Exception(f"Error downloading data: {e}")

    def upload_file(self, bucket_name, object_name, file_path):
        """
        Uploads a file to the specified S3 bucket and object.
        """
        try:
            self.s3_resource.Bucket(bucket_name).upload_file(file_path, object_name)
        except Exception as e:
            raise Exception(f"Error uploading file: {e}")
        
    def save_dataframe_to_csv(self, bucket_name, object_name, dataframe):
        """
        Saves a pandas DataFrame to a CSV file and uploads it to the specified S3 bucket.
        """
        try:
            csv_buffer = BytesIO()
            dataframe.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            self.s3_resource.Bucket(bucket_name).put_object(Key=object_name, Body=csv_buffer.getvalue())
        except Exception as e:
            raise Exception(f"Error saving DataFrame to CSV and uploading: {e}")