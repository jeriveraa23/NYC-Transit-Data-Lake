from extractors.nyc_extractor import NYCTaxiExtractor
from loaders.s3_loader import S3Loader

class NYCTaxiIngestionJob:
    def __init__(self, bucket_name):
        self.extractor = NYCTaxiExtractor()
        self.loader    = S3Loader(bucket_name)
    
    def run(self, year, month):
        """Pipeline Orchestration: Extract -> Load"""
        content, file_name = self.extractor.download_parquet(year, month)

        s3_key = f"bronze/yellow_taxi/{year}/{month:02d}/{file_name}"

        self.loader.upload_parquet(content, s3_key)

        print(f"Job finalized for the period {year}-{month}")