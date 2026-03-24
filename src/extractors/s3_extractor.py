import boto3
import io
from botocore.exceptions import ClientError

class S3Extractor:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')

    def extract_from_bronze(self, s3_key):
        """Read a file from bronze layer and bytes return"""
        try:
            print(f"S3Extractor: Extracting object from {s3_key}")
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            return response['Body'].read()
        except ClientError as e:
            print(f"Error reading from S3: {e}")
            raise