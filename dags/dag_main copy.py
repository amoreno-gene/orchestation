import logging
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.task_group import TaskGroup
from fivetran_provider_async.operators import FivetranOperator
from pendulum import datetime
import os
import importlib.util
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from datetime import datetime as dt, timedelta
from airflow.models import Variable
import json


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import functions from other modules
from uploaders.upload_to_blob import upload_to_azure_blob
from integrators.check_before_running_dbt_cloud_job import create_dbt_job_task

# Define your parameters here
DBT_CLOUD_CONN_ID = "dbt_cloud"
JOB_ID = "663697"
AIRBYTE_CONNECTION_ID = Variable.get("BLOB_TO_SNOWFLAKE_CONN")
date = "{{ ds_nodash }}"
extractors_dir = os.path.join(os.path.dirname(__file__), "extractors")

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

def upload_to_azure_blob(file_path: str, folder_path: str) -> None:
    azurehook = WasbHook(wasb_conn_id="azure_blob")
    container_name = "intermediate"
    
    # Read the content of the CSV file
    with open(file_path, 'r') as file:
        data = file.read()
    
    # Determine the full path for the blob
    blob_name = f"{folder_path}/{os.path.basename(file_path)}"
    
    # Upload the content to Azure Blob Storage
    azurehook.load_string(string_data=data, container_name=container_name, blob_name=blob_name)


@dag(
    start_date=datetime(2022, 2, 10),
    schedule="@daily",
    catchup=False,
)
def data_to_azure_blob_fivetran_dbt():
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
                # Determine the folder path in the container, based on the local file path relative to 'extractors' folder
                folder_path = os.path.relpath(file_path, start=extractors_dir).replace(os.path.basename(file_path), "").replace("\\", "/")
                upload_to_azure_blob(csv_file, folder_path)
                logger.info(f"Completed extraction and upload for {module_name}")

            extract_and_upload()

    mysql_sync = FivetranOperator(
            task_id="fivetran_op_async_mysql",
	        wait_for_completion="True",
	        fivetran_conn_id="fivetran_conn",
	        connector_id='indivisible_treatment',
            deferrable=False
    )

    azure_sync = FivetranOperator(
            task_id="fivetran_op_async_azure",
	        wait_for_completion="True",
	        fivetran_conn_id="fivetran_conn",
	        connector_id='indivisible_treatment',
            deferrable=False
    )


    end = DummyOperator(task_id="end")

    t0 >> data_group >> mysql_sync >> azure_sync >> end

data_to_azure_blob_fivetran_dbt()
