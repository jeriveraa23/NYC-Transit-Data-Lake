from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.jobs.nyc_ingestion_job import NYCTaxiIngestionJob

BUCKET_NAME = "nyc-transit-data-lake"

def execute_ingestion_task(year, month, **kwargs):
    print(f"Initiating modular ingestion process for the period {year}-{month}")

    job = NYCTaxiIngestionJob(bucket_name=BUCKET_NAME)

    job.run(int(year), int(month))

default_args = {
    'owner': 'Jorge Rivera',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ingestion_nyc_v2_modular',
    default_args=default_args,
    description='Monthly NYC Taxi data ingestion to AWS S3 (Bronze Layer)',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@monthly',
    catchup=True,          
    max_active_runs=1,     
    tags=['nyc', 's3', 'ingestion', 'modular'],
) as dag:
    
    ingest_task = PythonOperator(
        task_id='ingest_to_s3_task',
        python_callable=execute_ingestion_task,
        op_kwargs={
            'year': "{{ dag_run.logical_date.year }}",
            'month': "{{ dag_run.logical_date.month }}"
        }
    )