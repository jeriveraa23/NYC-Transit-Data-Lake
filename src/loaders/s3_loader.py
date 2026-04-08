import boto3
from botocore.exceptions import ClientError

class S3Loader:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.client = boto3.client('s3')

    def upload_parquet(self, file_stream, s3_key):
        """Upload parquet file to S3 using streaming"""

        try:
            self.client.upload_fileobj(
                Fileobj=file_stream,
                Bucket=self.bucket_name,
                Key=s3_key
            )

            print(f"Uploaded to s3://{self.bucket_name}/{s3_key}")
            return s3_key
        
        except ClientError as e:
            print(f"ERROR uploading to S3: {e}")
            raise