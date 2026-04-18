from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
from pyspark.sql import SparkSession


PROJECT_DIR = "/opt/airflow"
sys.path.append(os.path.join(PROJECT_DIR))

from src.jobs.nyc_taxi_job import run_nyc_pipeline
from src.jobs.nyc_ml_job import run_ml_pipeline

BUCKET_NAME = "nyc-transit-data-lake"

default_args = {
    'owner': 'Jorge Rivera',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def run_full_pipeline(year, month, bucket_name):
    spark = None
    try:
        spark = SparkSession.builder \
            .appName(f"NYC_Full_Pipeline_{year}_{month}") \
            .config("spark.driver.memory", "3g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.maxResultSize", "1g") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .getOrCreate()

        run_nyc_pipeline(spark, year, month, bucket_name)
        run_ml_pipeline(spark, year, month, bucket_name)

    except Exception as e:
        print(f"Critical error in full pipeline: {str(e)}")
        raise
    finally:
        if spark is not None:
            print("Closing Spark session")
            spark.stop()

with DAG(
    dag_id='nyc_taxi_pipeline_monolithic',
    default_args=default_args,
    description='Pipeline completo NYC Taxi (Bronze → Silver → Gold → ML)',
    start_date=datetime(2026, 2, 1),
    schedule_interval='@monthly',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['nyc', 'spark', 's3', 'ml'],
) as dag:
    pipeline_task = PythonOperator(
        task_id='run_full_pipeline',
        python_callable=run_full_pipeline,
        op_kwargs={
            'year': "{{ dag_run.logical_date.year }}",
            'month': "{{ dag_run.logical_date.month }}",
            'bucket_name': BUCKET_NAME
        }
    )
