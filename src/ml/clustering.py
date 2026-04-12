from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

class NYCClusterer:
    def __init__(self, spark_session: SparkSession, n_clusters: int = 4):
        self.spark = spark_session
        self.n_clusters = n_clusters
        self.model = None

    def traint(self, features_df:DataFrame):

        kmenas = KMeans(
            featuresCol="features",
            predictionCol="clusters",
            k=self.n_clusters,
            seed=42,
            maxIter=20
        )

        self.model = kmenas.fit(features_df)

        predictions = self.model.transform(features_df)
        evaluator = ClusteringEvaluator(
            featuresCol="features",
            predictionCol="cluster",
            metricName="silhouette"
        )
        silhouette = evaluator.evaluate(predictions)
        print(f"[Clusterer] Silhouette score: {silhouette:.4f} (ideal > 0.5)")

        return self.model
    
    def predict(self, features_df: DataFrame) -> DataFrame:
        if self.model is None:
            raise ValueError("The model hasn't been trained. First call train()")
        
        return self.model.transform(features_df).select(
            "PULocationID", "cluster", "avg_revenue", "avg_distance",
            "avg_duration_minutes", "avg_tip_efficiency", "total_trips"
        )
    
    def label_clusters(self, clustered_df: DataFrame) -> DataFrame:

        cluster_stats = clustered_df.groupBy("cluster").agg(
            F.avg("avg_revenue").alias("revenue_mean"),
            F.avg("avg_distance").alias("distance_mean"),
            F.avg("total_trips").alias("trips_mean"),
            F.avg("avg_tip_efficiency").alias("trip_mean")
        )

        labeled = cluster_stats.withColumn(
            "cluster_label",
            F.when((F.col("revenue_mean") > 20) & (F.col("distance_mean") > 5), "Zona Premium")
             .when((F.col("trips_mean") > 5000) & (F.col("distance_mean") < 3), "Zona Alta Frecuencia")
             .when(F.col("tip_mean") > 15, "Zona Alta Propina")
             .otherwise("Zona Estándar")
        )

        return clustered_df.join(
            labeled.select("cluster", "cluster_label"), on="cluster", how="left"
        )
    
    def save_clusters(self, clustered_df: DataFrame, bucket_name: str):

        output_path = f"s3a://{bucket_name}/ml/clusters"

        clustered_df.write \
            .mode("overwrite") \
            .parquet(output_path)
        
        print(f"[Clusterer] The clusters has been saved in {output_path}")