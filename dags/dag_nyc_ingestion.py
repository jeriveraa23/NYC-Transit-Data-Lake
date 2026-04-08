from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os



PROJECT_DIR = "/opt/airflow"
sys.path.append(os.path.join(PROJECT_DIR))
from src.jobs.nyc_taxi_job import run_nyc_pipeline

BUCKET_NAME = "nyc-transit-data-lake"

default_args = {
    'owner': 'Jorge Rivera',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='nyc_taxi_pipeline_monolithic',
    default_args=default_args,
    description='Pipeline completo NYC Taxi (Bronze → Silver → Gold)',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@monthly',
    catchup=True,
    max_active_runs=1,
    concurrency=1,
    tags=['nyc', 'spark', 's3'],
) as dag:
    ingest_task = PythonOperator(
        task_id='run_full_pipeline',
        python_callable=run_nyc_pipeline,
        op_kwargs={
            'year': "{{ dag_run.logical_date.year }}",
            'month': "{{ dag_run.logical_date.month }}",
            'bucket_name': BUCKET_NAME
        }
    )