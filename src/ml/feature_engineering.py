from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.sql.types import DoubleType

class NYCFeatureEngineer:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
    
    def build_regression_features(self, silver_df:DataFrame) -> DataFrame:
        
        df = silver_df.withColumn(
            "pickup_hour", F.hour("tpep_pickup_datetime")
        ).withColumn(
            "pickup_day_of_week", F.dayofweek("tpep_pickup_datetime")
        ).withColumn(
            "PULocationID", F.col("PULocationID").cast(DoubleType())
        ).withColumn(
            "DOLocationID", F.col("DOLocationID").cast(DoubleType())
        ).withColumn(
            "trip_distance", F.col("trip_distance").cast(DoubleType())
        ).withColumn(
            "trip_duration_minutes", F.col("trip_duration_minutes").cast(DoubleType())
        ).withColumn(
            "passenger_count", F.col("passenger_count").cast(DoubleType())
        ).withColumn(
            "total_amount", F.col("total_amount").cast(DoubleType())
        )

        # Remove nulls from key columns
        df = df.dropna(subset=[
            "trip_distance", "trip_duration_minutes", "total_amount",
            "PULocationID", "DOLocationID", "pickup_hour", "pickup_day_of_week"
        ])

        feature_cols = [
            "trip_distance", "trip_duration_minutes", "passenger_count",
            "PULocationID", "DOLocationID", "pickup_hour", "pickup_day_of_week"
        ]

        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
        df = assembler.transform(df)

        scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
        scaler_model = scaler.fit(df)
        df = scaler_model.transform(df)

        return df.select("features", "total_amount", "tpep_pickup_datetime", "PULocationID")
    
    def build_classification_features(self, silver_df: DataFrame) -> DataFrame:
        df = silver_df.withColumn(
            "tip_pct", F.col("tip_amount") / F.col("fare_amount") * 100
        ).withColumn(
            "tip_label",
            F.when(F.col("tip_pct") > 20, "Alta")
             .when(F.col("tip_pct") >= 5, "Media")
             .otherwise("Baja")
        ).withColumn(
            "pickup_hour", F.hour("tpep_pickup_datetime")
        ).withColumn(
            "pickup_day_of_week", F.dayofweek("tpep_pickup_datetime")
        )

        # Cast to Double
        for col_name in ["trip_distance", "trip_duration_minutes", "passenger_count",
                         "PULocationID", "DOLocationID", "payment_type", "RatecodeID"]:
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

        df = df.dropna(subset=["tip_label", "trip_distance", "fare_amount"])

        # Convert string label to numeric index
        indexer = StringIndexer(inputCol="tip_label", outputCol="label")
        df = indexer.fit(df).transform(df)
        
        feature_cols = [
            "trip_distance", "trip_duration_minutes", "passenger_count",
            "PULocationID", "DOLocationID", "pickup_hour", "pickup_day_of_week",
            "payment_type", "RatecodeID"
        ]

        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df = assembler.transform(df)

        return df.select("features", "label", "tip_label", "tpep_pickup_datetime", "PULocationID")
    
    def build_clustering_features(self, gold_df:DataFrame) -> DataFrame:
        df = gold_df.groupBy("PULocationID").agg(
            F.avg("avg_revenue").alias("avg_revenue"),
            F.avg("avg_distance").alias("avg_distance"),
            F.avg("avg_duration_minutes").alias("avg_duration_minutes"),
            F.avg("tip_efficiency_pct").alias("avg_tip_efficiency"),
            F.sum("total_trips").alias("total_trips")
        )

        for col_name in ["avg_revenue", "avg_distance", "avg_duration_minutes",
                         "avg_tip_efficiency", "total_trips"]:
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
    
        df = df.dropna()

        feature_cols = ["avg_revenue", "avg_distance", "avg_duration_minutes",
                        "avg_tip_efficiency", "total_trips"]
        
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
        df = assembler.transform(df)

        scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
        scaler_model = scaler.fit(df)
        df = scaler_model.transform(df)

        return df.select("PULocationID", "features", "avg_revenue", "avg_distance",
                         "avg_duration_minutes", "avg_tip_efficiency", "total_trips")
    

    def build_forecasting_features(self, gold_df: DataFrame):

        daily_df = gold_df.groupBy("pickup_date").agg(
            F.sum("total_trips").alias("y")
        ).orderBy("pickup_date")
        pandas_df = daily_df.toPandas()
        pandas_df = pandas_df.rename(columns={"pickup_date": "ds"})
        pandas_df["ds"] = pandas_df["ds"].astype(str)

        return pandas_df