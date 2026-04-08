import os
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

class S3Extractor:
    def __init__(self, spark_session: SparkSession, bucket_name: str):
        self.bucket_name = bucket_name
        self.spark       = spark_session

    def read_parquet(self, s3_key:str):
        """Reads a parquet file directly from S3 into a Spark DataFrame"""

        s3_path = f"s3a://{self.bucket_name}/{s3_key}"

        print(f"S3Extractor: Trying read data from {s3_path}")

        try:
            df = self.spark.read.parquet(s3_path)

            if df.rdd.isEmpty():
                print(f"WARNING: Empty file at {s3_path}")

            return df
        
        except AnalysisException as e:
            print(f"ERROR: File not found or unreadable in S3 -> {e}")
            raise
        except Exception as e:
            print(f"ERROR: Unexpected S3 error -> {e}")
            raise
        