from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

class NYCClassifier:

    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.model = None

    def train(self, features_df: DataFrame):

        train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)

        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            numTrees=100,
            maxDepth=10,
            seed=42
        )

        self.model = rf.fit(train_df)

        predictions = self.model.transform(test_df)

        acc_evaluator = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="accuary"
        )
        accuary = acc_evaluator.evaluate(predictions)

        f1_evaluator = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="f1"
        )

        f1 = f1_evaluator.evaluate(predictions)

        print(f"[Classifier] Accuery in test: {accuary:.4f}")
        print(f"[Classifier] F1-score en test: {f1:.4f}")

        return self.model
    
    def predict(self, features_df:DataFrame) -> DataFrame:

        if self.model is None:
            raise ValueError("The model hasn't been trained. First call train()")
        
        predictions = self.model.transform(features_df)

        extract_max_prob = F.udf(lambda v: float(max(v)), "double")

        return predictions.withColumn(
            "prediction_confidence", extract_max_prob(F.col("probability"))
        ).select(
            "tpep_pickup_datetime", "PULocationID",
            "tip_label", "prediction", "prediction_confidence"
        )
     
    def save_predictions(self, predictions_df: DataFrame, bucket_name: str, year: int, month: int):
        
        output_path = f"s3a://{bucket_name}/ml/predictions/classification/year={year}/month={month:02d}"

        predictions_df.write \
            .mode("overwrite") \
            .parquet(output_path)
        
        print(f"[Classifier] The predictions are saved in {output_path}")