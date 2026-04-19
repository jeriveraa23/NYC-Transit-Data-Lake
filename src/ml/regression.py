from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

class NYCRegressor:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.model = None

    def train(self, features_df: DataFrame):

        features_df = features_df.limit(100000)

        train_df, test_df = features_df.randomSplit([0.8, 0.2],seed=42)

        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol="total_amount",
            numTrees=10,     
            maxDepth=4,        
            maxBins=32,
            subsamplingRate=0.5,
            seed=42
        )

        train_df = train_df.cache()
        train_df.count()
        self.model = rf.fit(train_df)

        predictions = self.model.transform(test_df)
        evaluator = RegressionEvaluator(
            labelCol="total_amount",
            predictionCol="prediction",
            metricName="rmse"
        )
        rmse = evaluator.evaluate(predictions)
        print(f"[Regressor] RMSE en test: {rmse:.4f}")

        return self.model
    
    def predict(self, features_df: DataFrame) -> DataFrame:

        if self.model is None:
            raise ValueError("The model hasn't been trained. first call train()")
        
        predictions = self.model.transform(features_df)

        return predictions.withColumnRenamed("prediction", "predicted_total_amount") \
                            .select("tpep_pickup_datetime", "PULocationID",
                                    "total_amount", "predicted_total_amount")
    
    def save_predictions(self, predictions_df: DataFrame, bucket_name: str, year: int, month: int):

        output_path = f"s3a://{bucket_name}/ml/predictions/regression/year={year}/month={month:02d}/"

        predictions_df.write \
            .mode("overwrite") \
            .parquet(output_path)
        
        print(f"[Regressor] The predictions are saved in {output_path}")