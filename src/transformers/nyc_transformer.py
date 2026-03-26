from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, BooleanType, TimestampType

class NYCTaxiTransformer:
    def __init__(self, spark_session):
        self.spark = spark_session

    def transform_to_silver(self, raw_df):
        print("Spark: Performing deep cleaning for silver layer")

        # 1. Initial cleaning of nulls and basic types (IDs & Flags)
        df = raw_df.withColumn("VendorID", F.col("VendorID").cast(IntegerType())) \
            .withColumn("tpep_pickup_datetime", (F.col("tpep_pickup_datetime") / 1000).cast(TimestampType())) \
            .withColumn("tpep_dropoff_datetime", (F.col("tpep_dropoff_datetime") / 1000).cast(TimestampType())) \
            .withColumn("RatecodeID", F.col("RatecodeID").cast(IntegerType())) \
            .withColumn("PULocationID", F.col("PULocationID").cast(IntegerType())) \
            .withColumn("DOLocationID", F.col("DOLocationID").cast(IntegerType())) \
            .withColumn("payment_type", F.col("payment_type").cast(IntegerType())) \
            .withColumn("store_and_fwd_flag", F.when(F.col("store_and_fwd_flag") == "Y", True).otherwise(False).cast(BooleanType()))

        # 2. Passenger and distnace logic
        # Those that are 0 are set to 1 and delete records with distance <=0
        df = df.withColumn("passenger_count", F.when(F.col("passenger_count") == 0, 1)
                           .otherwise(F.col("passenger_count")).cast(IntegerType())) \
               .filter(F.col("trip_distance") > 0)

        # 3. Treatment of nulls and financial casting (Decimal 10,2)
        # If they come out as null, put 0.0
        financial_cols = [
            "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", 
            "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee"
        ]
        
        for col_name in financial_cols:
            df = df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0.0)).cast(DecimalType(10, 2)))

        # 4. Especific validations
        # Ratecode between 1 and 6 | fare_amount > 0
        df = df.filter((F.col("RatecodeID") >= 1) & (F.col("RatecodeID") <= 6)) \
               .filter(F.col("fare_amount") > 0)

        # 5. Tipping logic (tip_amount)
        # If it is not a card
        df = df.withColumn("tip_amount", 
                           F.when(F.col("payment_type") == 1, F.col("tip_amount"))
                           .otherwise(F.lit(0.0)))

        # 6. Total verification
        # We calculate he sum of the components
        sum_expression = (
            F.col("fare_amount") + F.col("extra") + F.col("mta_tax") + 
            F.col("tip_amount") + F.col("tolls_amount") + 
            F.col("improvement_surcharge") + F.col("congestion_surcharge") + 
            F.col("airport_fee")
        )

        df = df.withColumn("calculated_total", sum_expression.cast(DecimalType(10, 2))) \
               .withColumn("is_total_correct", 
                           F.when(F.col("total_amount") == F.col("calculated_total"), True)
                           .otherwise(False))

        # 7. Cut date
        df = df.withColumn("processed_at", F.current_timestamp())

        return df
    
    def transform_to_gold(self, silver_df):
        print(f"Spark: Generating business metrics for gold layer")


        # Aggregation.
        # Grouping by Date and by Zone
        gold_df = silver_df.groupBy(
            F.to_date(F.col("tpep_pickup_datetime")).alias("pickup_date"),
            F.col("PULocationID")
        ).agg(
            F.count("VendorID").alias("total_trips"),

            F.sum(F.when(F.col("is_total_correct") == False, 1).otherwise(0)).alias("flagged_incorrect_trips"),

            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_revenue"),
            F.sum("tip_amount").alias("total_tips_profit"),

            (F.sum("tip_ammount") / F.su,("fare_amount") * 100).alias("tip_efficiency_pct"),

            F.avg("trip_distance").alias("avg_distance"),
            F.sum("passenger_count").alias("total_passengers_transported"),
            F.avg("trip_duration_minutes").alias("avg_duration_minutes")

        )

        # Trust field
        # Calculing what percentage of the day's data is reliable
        gold_df = gold_df.withColumn(
            "data_trust_score",
            F.round((1 - (F.col("flagged_incorrect_trips") / F.col("total_trips")) * 100, 2))
        )

        # Final rounding of metrics
        cols_to_round = [
            "total_revenue", "avg_revenue", "total_tips_profit", "tip_efficiency_pct", "avg_distance", "avg_duration_minutes"
        ]

        for col_name in cols_to_round:
            gold_df = gold_df.withColumn(col_name, F.round(F.col(col_name), 2))

        # ordering
        gold_df = gold_df.orderBy(F.col("pickup_date").desc(), F.col("total_trips").desc())

        return gold_df