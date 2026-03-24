from extractors.nyc_extractor import NYCTaxiExtractor
from loaders.s3_loader import S3Loader

class NYCTaxiIngestionJob:
    def __init__(self, bucket_name):
        self.extractor = NYCTaxiExtractor()
        self.loader    = S3Loader(bucket_name)
    
    def run(self, year, month):
        """Pipeline Orchestration: Extract -> Load"""
        content, file_name = self.extractor.download_parquet(year, month)

        self.loader.upload_to_bronze(content, year, month, file_name)

        print(f"Job finalized for the period {year}-{month}")