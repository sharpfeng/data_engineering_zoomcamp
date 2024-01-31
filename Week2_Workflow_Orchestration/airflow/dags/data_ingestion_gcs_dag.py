import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_file = "yellow_tripdata_2019-01.csv.gz"
dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'ny_taxi') # I defined this GCS resource in week 1 in ../Week1_Docker_Terraform/terraform/variables.tf


def format_to_parquet(src_file):
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
            },
            "schema": {
                 "fields": [
                        {
                            "mode": "NULLABLE",
                             "name": "VendorID",
                            "type": "INTEGER"
                        },
                        {
                            "mode": "NULLABLE",
                            "name": "tpep_pickup_datetime",
                            "type": "TIMESTAMP"
                        },
                        {
                            "mode": "NULLABLE",
                            "name": "tpep_dropoff_datetime",
                            "type": "TIMESTAMP"
                        },
                        {
                            "mode": "NULLABLE",
                            "name": "passenger_count",
                            "type": "INTEGER"
                        },
                        {
                            "mode": "NULLABLE",
                            "name": "trip_distance",
                            "type": "FLOAT"
                        },
                        {
                            "mode": "NULLABLE",
                            "name": "RatecodeID",
                            "type": "INTEGER"
                        },
                        {
                            "mode": "NULLABLE",
                            "name": "store_and_fwd_flag",
                            "type": "STRING"
                        },
                        {
                            "mode": "NULLABLE",
                            "name": "PULocationID",
                            "type": "INTEGER"
                        },
                        {
                            "mode": "NULLABLE",
                            "name": "DOLocationID",
                            "type": "INTEGER"
                        },
                        {
                            "mode": "NULLABLE",
                            "name": "payment_type",
                            "type": "INTEGER"
                        },
                        {
                            "mode": "NULLABLE",
                            "name": "fare_amount",
                            "type": "FLOAT64"
                        },
                        {
                            "mode": "NULLABLE",
                            "name": "extra",
                            "type": "FLOAT64"
                        },
                        {
                            "mode": "NULLABLE",
                            "name": "mta_tax",
                            "type": "FLOAT64"
                        },
                        {
                            "mode": "NULLABLE",
                            "name": "tip_amount",
                            "type": "FLOAT64"
                        },
                        {
                            "mode": "NULLABLE",
                            "name": "tolls_amount",
                            "type": "FLOAT64",
                        },
                        {
                            "mode": "NULLABLE",
                            "name": "improvement_surcharge",
                            "type": "FLOAT64"
                        },
                        {
                            "mode": "NULLABLE",
                            "name": "total_amount",
                            "type": "FLOAT64"
                        },
                        {
                            "mode": "NULLABLE",
                            "name": "congestion_surcharge",
                            "type": "FLOAT64"
                        }
                 ]
            },  
        },        
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task