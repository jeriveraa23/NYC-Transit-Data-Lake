from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta

# Import classes
from extractors.nyc_extractor import NYCTaxiExtractor
from extractors.s3_extractor import S3Extractor
from loaders.s3_loader import S3Loader
from transformers.nyc_transformer import NYCTaxiTransformer

def run_nyc_pipeline(spark, year, month, bucket_name):

    requested = datetime(int(year), int(month), 1)
    cutoff = datetime.now() - timedelta(days=90)

    year  = int(year)
    month = int(month)

    if requested > cutoff:
        print(f"Skipping {year}-{month:02d}: data not yet available from NYC TLC")
        return
    
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    # Instantiate tools
    web_extractor = NYCTaxiExtractor()
    s3_extractor = S3Extractor(spark, bucket_name)
    s3_loader     = S3Loader(bucket_name)
    transformer    = NYCTaxiTransformer(spark)

    print(f"=== STARTING PIPELINE FOR {year}-{month:02d} ===")

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
        silver_df = transformer.transform_to_silver(raw_df, year, month)

        silver_df = silver_df.withColumn("year", F.year("tpep_pickup_datetime")) \
                             .withColumn("month", F.month("tpep_pickup_datetime"))
        
        silver_df = silver_df.repartition("year", "month")

        # Save directly to silver layer
        silver_df.write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(f"s3a://{bucket_name}/silver/")

        # --- STEP 3: SILVER TO GOLD ---
        print(f"Transforming gold layer (KPIs and Metrics)")

        # Transform to gold (Aggregations and trust score)
        gold_df = transformer.transform_to_gold(silver_df)

        #Save gold to S3
        gold_df = gold_df.withColumn("year", F.year("pickup_date")) \
                         .withColumn("month", F.month("pickup_date"))
        
        gold_df = gold_df.repartition("year", "month")
        
        gold_df.write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(f"s3a://{bucket_name}/gold/")

        print(f"Pipeline successfully completed for {year}-{month:02d}")
        gold_df.show(5)
    except Exception as e:
        print(f" Critical error in job {str(e)}")
        raise