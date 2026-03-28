import os
from pyspark.sql import SparkSession
import sys

# Import classes
from extractors.nyc_extractor import NYCTaxiExtractor
from extractors.s3_extractor import S3Extractor
from loaders.s3_loader import S3Loader
from transformers.nyc_transformer import NYCTaxiTransformer

def run_nyc_pipeline(year, month, bucket_name):
    # Initialize spark
    spark = SparkSession.builder \
        .appName(f"NYC_Taxi_Pipeline_{year}_{month}") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()
    
    # Instantiate tools
    web_extractor = NYCTaxiExtractor()
    s3_extractor  = S3Extractor(bucket_name)
    s3_loader     = S3Loader(bucket_name)
    transformer    = NYCTaxiTransformer(spark)

    print(f"Starting full flow for {year}-{month}")

    try:
        # --- STEP 1: WEB TO S3 BRONZE ---
        print(f"1. Downloading from original web")
        data_bytes, file_name = web_extractor.download_parquet(year,month)

        bronze_key = f"bronze/year={year}/month={month:02d}/{file_name}"
        s3_loader.upload_parquet(data_bytes, bronze_key)

        # ---  STEP 2: BRONZE TO SILVER ---
        print(f"2. Processing silver layer")
        # Download bronze info from our S3
        raw_df = s3_extractor.read_parquet(bronze_key)

        # Transform to silver (Clean and type casts)
        silver_df = transformer.transform_to_silver(raw_df)

        # Save directly to silver layer
        silver_key = f"silver/year={year}/month={month:02d}/yellow_trips_cleaned.parquet"
        silver_df.write.mode("overwrite").parquet(f"s3a://{bucket_name}/{silver_key}")

        # --- STEP 3: SILVER TO GOLD ---
        print(f"Transforming gold layer (KPIs and Metrics)")
        silver_df = s3_extractor.read_parquet(silver_key)

        # Transform to gold (Aggregations and trust score)
        gold_df = transformer.transform_to_gold(silver_df)

        #Save gold to S3
        gold_key = f"gold/year={year}/month={month:02d}/performance_metrics.parquet"
        gold_df.write.mode("overwrite").parquet(f"s3a://{bucket_name}/{gold_key}")

        print(f"Pipeline successfully completed for {year}-{month:02d}")
        gold_df.show(5)
    except Exception as e:
        print(f" Critical error in job {str(e)}")
        raise
    finally:
        print(f"Closing spark session")
        spark.stop()

if __name__ == "__main__":
    BUCKET = "nyc-transit-data-lake"
    

    if len(sys.argv) == 3:
        YEAR  = int(sys.argv[1])
        MONTH = int(sys.argv[2])
    else:

        YEAR  = 2023
        MONTH = 1
    
    run_nyc_pipeline(YEAR, MONTH, BUCKET)