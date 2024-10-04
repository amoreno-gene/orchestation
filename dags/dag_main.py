from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
import os
import importlib.util
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable
from datetime import datetime as dt
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define your parameters here
DBT_CLOUD_CONN_ID = "dbt_cloud"
JOB_ID = "663697"
date = "{{ ds_nodash }}"
extractors_dir = os.path.join(os.path.dirname(__file__), "extractors")

# Google Cloud Storage bucket
GCS_BUCKET_NAME = "intermediate"

# Function to dynamically import a module
def import_module_from_path(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# Find all extraction scripts
extractor_scripts = []
for root, dirs, files in os.walk(extractors_dir):
    for file in files:
        if file.endswith(".py") and file.startswith("extract_"):
            file_path = os.path.join(root, file)
            module_name = file[:-3]
            extractor_scripts.append((module_name, file_path))

# Function to upload a file to Google Cloud Storage
def upload_to_gcs(file_path: str, folder_path: str) -> None:
    gcs_hook = GCSHook()
    with open(file_path, 'r') as file:
        data = file.read()
    blob_name = f"{folder_path}/{os.path.basename(file_path)}"
    gcs_hook.upload(bucket_name=GCS_BUCKET_NAME, object_name=blob_name, data=data)
    logger.info(f"Uploaded {file_path} to gs://{GCS_BUCKET_NAME}/{blob_name}")

# DAG definition
@dag(
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
)
def data_to_gcs_airbyte_dbt():
    t0 = DummyOperator(task_id="start")

    with TaskGroup("data_task_group") as data_group:
        for module_name, file_path in extractor_scripts:

            @task(task_id=f"extract_and_upload_{module_name}")
            def extract_and_upload(module_name=module_name, file_path=file_path, date=date):
                logger.info(f"Starting extraction and upload for {module_name}")
                module = import_module_from_path(module_name, file_path)
                csv_file = module.extract_and_process_data()  # Call the extraction function
                timestamp = dt.now().strftime("%Y%m%d%H%M%S")
                filename = f"{os.path.splitext(os.path.basename(csv_file))[0]}_{timestamp}.csv"
                folder_path = os.path.relpath(file_path, start=extractors_dir).replace(os.path.basename(file_path), "").replace("\\", "/")
                upload_to_gcs(csv_file, folder_path)
                logger.info(f"Completed extraction and upload for {module_name}")

            extract_and_upload()

    # Comentar o eliminar las siguientes líneas para desactivar Airbyte
    # token = get_new_token()
    # trigger_sync = SimpleHttpOperator(...)
    # wait_for_sync_to_complete = PythonOperator(...)

    end = DummyOperator(task_id="end")

    t0 >> data_group >> end

data_to_gcs_airbyte_dbt()

