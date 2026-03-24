import boto3
from botocore.exceptions import ClientError

class S3Loader:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.client = boto3.client('s3')

    def upload_parquet(self, data_bytes, s3_key):
        """Upload files organized by partitions"""

        try:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=data_bytes
            )
            print(f"File successfully uploaded to: s3://{self.bucket_name}/{s3_key}")
            return s3_key
        except ClientError as e:
            print(f"Loading error to S3: {e}")
            raise