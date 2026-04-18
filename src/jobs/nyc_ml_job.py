from pyspark.sql import functions as F
from datetime import datetime
from dateutil.relativedelta import relativedelta
 
from extractors.s3_extractor import S3Extractor
from ml.feature_engineering import NYCFeatureEngineer
from ml.regression import NYCRegressor
from ml.classification import NYCClassifier
from ml.clustering import NYCClusterer
from ml.forecasting import NYCForecaster

def run_ml_pipeline(spark, year, month, bucket_name):
    
    year  = int(year)
    month = int(month)

    print(f"=== STARTING ML PIPELINE FOR {year}-{month:02d} ===")

    train_silver_df   = None
    predict_silver_df = None
    train_gold_df     = None
    full_gold_df      = None

    try:
        feature_engineer = NYCFeatureEngineer(spark)

        # TWO-YEAR WINDOW
        current_month = datetime(year, month, 1)
        cutoff_date   = current_month - relativedelta(years=2)

        print(f"Training Window: {cutoff_date.strftime('%Y-%m')} → {year}-{month:02d}")

        # READ ALL OF SILVER
        print(f"\nReading silver from S3...")
        full_silver_df = spark.read.parquet(f"s3a://{bucket_name}/silver/")

        # Training: Last 2 years without the current month
        train_silver_df = full_silver_df.filter(
            (F.col("tpep_pickup_datetime") >= F.lit(cutoff_date.strftime("%Y-%m-%d"))) &
            ~(
                (F.year("tpep_pickup_datetime") == year) &
                (F.month("tpep_pickup_datetime") == month)
            )
        )

        # Prediction: current month only
        predict_silver_df = full_silver_df.filter(
            (F.year("tpep_pickup_datetime") == year) &
            (F.month("tpep_pickup_datetime") == month)
        )

        train_silver_df.cache()
        predict_silver_df.cache()

        print(f"Training records {train_silver_df.count():,}")
        print(f"Records to predict (current month): {predict_silver_df.count():,}")

        # READ ALL OF GOLD
        print("\nReading gold from S3...")
        full_gold_df = spark.read.parquet(f"s3a://{bucket_name}/gold/")

        # Gold last 2 years including current month -> for clustering
        train_gold_df = full_gold_df.filter(
            F.col("pickup_date") >= F.lit(cutoff_date.strftime("%Y-%m-%d"))
        )

        train_gold_df.cache()
        full_gold_df.cache()

        # --- 1. REGRESSION ---
        print("\n[1/4] Regression: prediction of total_amount")
        reg_train_features    = feature_engineer.build_regression_features(train_silver_df)
        reg_predict_features  = feature_engineer.build_regression_features(predict_silver_df)

        regressor = NYCRegressor(spark)
        regressor.train(reg_train_features)
        regression_predictions = regressor.predict(reg_predict_features)
        regressor.save_predictions(regression_predictions, bucket_name, year, month)

        # --- 2. CLASSIFICATION ---
        print("\n[2/4] Classification: tip level")
        cls_train_features   = feature_engineer.build_classification_features(train_silver_df)
        cls_predict_features = feature_engineer.build_classification_features(predict_silver_df)
 
        classifier = NYCClassifier(spark)
        classifier.train(cls_train_features)
        classification_predictions = classifier.predict(cls_predict_features)
        classifier.save_predictions(classification_predictions, bucket_name, year, month)

        # --- 3. CLUSTERING ---
        print("\n[3/4] Clustering: zone segmentation")
        clustering_features = feature_engineer.build_clustering_features(train_gold_df)
 
        clusterer = NYCClusterer(spark, n_clusters=4)
        clusterer.train(clustering_features)
        clustered_df = clusterer.predict(clustering_features)
        labeled_df   = clusterer.label_clusters(clustered_df)
        clusterer.save_clusters(labeled_df, bucket_name)

        # --- 4. FORECASTING ---
        print("\n[4/4] Forecasting: future demand prediction")
        forecasting_pandas_df = feature_engineer.build_forecasting_features(full_gold_df)
 
        forecaster = NYCForecaster(forecast_days=30)
        forecaster.train(forecasting_pandas_df)
        forecast_df = forecaster.predict()
        forecaster.save_forecast(forecast_df, spark, bucket_name)

        print(f"\n=== ML PIPELINE COMPLETED FOR {year}-{month:02d} ===")
    
    except Exception as e:
        print(f"Critical error in ML job: {str(e)}")
        raise

    finally:
        if train_silver_df is not None:
            train_silver_df.unpersist()
        if predict_silver_df is not None:
            predict_silver_df.unpersist()
        if train_gold_df is not None:
            train_gold_df.unpersist()
        if full_gold_df is not None:
            full_gold_df.unpersist()