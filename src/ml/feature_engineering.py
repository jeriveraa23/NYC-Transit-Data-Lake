from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.pipeline import Pipeline
from pyspark.sql.types import DoubleType

class NYCFeatureEngineer:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
    
    def build_regression_features(self, silver_df:DataFrame) -> DataFrame:

        feature_cols = [
            "trip_distance", "trip_duration_minutes", "passenger_count",
            "PULocationID", "DOLocationID", "pickup_hour", "pickup_day_of_week"
        ]
        
        df = (
            silver_df
            .withColumn("pickup_hour",        F.hour("tpep_pickup_datetime"))
            .withColumn("pickup_day_of_week",  F.dayofweek("tpep_pickup_datetime"))
            .withColumn("PULocationID",        F.col("PULocationID").cast(DoubleType()))
            .withColumn("DOLocationID",        F.col("DOLocationID").cast(DoubleType()))
            .withColumn("trip_distance",       F.col("trip_distance").cast(DoubleType()))
            .withColumn("trip_duration_minutes", F.col("trip_duration_minutes").cast(DoubleType()))
            .withColumn("passenger_count",     F.col("passenger_count").cast(DoubleType()))
            .withColumn("total_amount",        F.col("total_amount").cast(DoubleType()))
            .dropna(subset=feature_cols + ["total_amount"])
        )

        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw", handleInvalid="skip")

        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withMean=False,  
            withStd=True
        )

        pipeline = Pipeline(stages=[assembler, scaler])

        sample_df = df.sample(fraction=0.1, seed=42)
        pipeline_model = pipeline.fit(sample_df)

        return pipeline_model.transform(df).select(
            "features", "total_amount", "tpep_pickup_datetime", "PULocationID"
        )

    
    def build_classification_features(self, silver_df: DataFrame) -> DataFrame:

        feature_cols = [
            "trip_distance", "trip_duration_minutes", "passenger_count",
            "PULocationID", "DOLocationID", "pickup_hour", "pickup_day_of_week",
            "payment_type", "RatecodeID"
        ]

        df = silver_df.withColumn(
            "tip_pct", F.col("tip_amount") / F.col("fare_amount") * 100
        ).withColumn(
            "tip_label",
            F.when(F.col("tip_pct") > 20, "Alta")
             .when(F.col("tip_pct") >= 5, "Media")
             .otherwise("Baja")
        ).withColumn(
            "pickup_hour",       F.hour("tpep_pickup_datetime")
        ).withColumn(
            "pickup_day_of_week", F.dayofweek("tpep_pickup_datetime")
        )

        for col_name in feature_cols + ["fare_amount"]:
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

        df = df.dropna(subset=["tip_label", "trip_distance", "fare_amount"])

        indexer  = StringIndexer(inputCol="tip_label", outputCol="label", handleInvalid="skip")
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")

        pipeline = Pipeline(stages=[indexer, assembler])
        sample_df = df.sample(fraction=0.1, seed=42)
        pipeline_model = pipeline.fit(sample_df)

        return pipeline_model.transform(df).select(
            "features", "label", "tip_label", "tpep_pickup_datetime", "PULocationID"
        )
    
    def build_clustering_features(self, gold_df:DataFrame) -> DataFrame:

        feature_cols = [
            "avg_revenue", "avg_distance", "avg_duration_minutes",
            "avg_tip_efficiency", "total_trips"
        ]
        
        df = gold_df.groupBy("PULocationID").agg(
            F.avg("avg_revenue").alias("avg_revenue"),
            F.avg("avg_distance").alias("avg_distance"),
            F.avg("avg_duration_minutes").alias("avg_duration_minutes"),
            F.avg("tip_efficiency_pct").alias("avg_tip_efficiency"),
            F.sum("total_trips").alias("total_trips")
        )

        for col_name in feature_cols:
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

        df = df.dropna()

        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw", handleInvalid="skip")

        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withMean=True,
            withStd=True
        )

        pipeline = Pipeline(stages=[assembler, scaler])
        sample_df = df.sample(fraction=0.1, seed=42)
        pipeline_model = pipeline.fit(sample_df)

        df = df.cache()
        df.count()

        return pipeline_model.transform(df).select(
            "PULocationID", "features", "avg_revenue", "avg_distance",
            "avg_duration_minutes", "avg_tip_efficiency", "total_trips"
        )
    
    def build_forecasting_features(self, gold_df: DataFrame):

        pandas_df = (
            gold_df
            .groupBy("pickup_date")
            .agg(F.sum("total_trips").alias("y"))
            .orderBy("pickup_date")
            .toPandas()
        )

        pandas_df = pandas_df.rename(columns={"pickup_date": "ds"})
        pandas_df["ds"] = pandas_df["ds"].astype(str)

        df = gold_df
        df = df.cache()

        return pandas_df