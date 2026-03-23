import requests
from connectors.s3_connector import S3Connector

class NYCTaxiIngestionJob:
    def __init_(self, bucket_name):
        self.bucket_name = bucket_name
        self.base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
        self.s3_connector = S3Connector(self.bucket_name)

    def _build_url(self, year, month):
        """Private method to build download URL"""
        file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
        return f"{self.base_url}/{file_name}", file_name
    
    def extract(self, url):
        """Extraction Logic"""
        print(f"Data request from: {url}")
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    
    def run(self, year, month):
        """Pipeline Orchestration: Extract -> Load"""
        url, file_name = self._build_url(year, month)
        s3_key = f"raw/{year}/{month:02d}/{file_name}"

        data_content = self.extract(url)

        self.s3_connector.upload_parquet(data_content, s3_key)

        print(f"Process successfully completed for {year}-{month}")