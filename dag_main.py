import logging
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
import os
import importlib.util
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.models import Variable
import json
import requests
from airflow.operators.python import PythonOperator
from datetime import datetime as dt, timedelta
import time

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
client_secret = 'GrdeWaQCbylzVbENIh53iKfrfn3Lgv1c'

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

# Function to get a new Airbyte token
@task
def get_new_token():
    try:
        client_id = Variable.get("AIRBYTE_CLIENT_ID")
        client_secret = 'GrdeWaQCbylzVbENIh53iKfrfn3Lgv1c'
        logger.info(f"Requesting new Airbyte token with: {client_id} and {client_secret}")
        if not client_id or not client_secret:
            raise ValueError("Client ID or Client Secret is not set in Airflow Variables.")
        
        url = "https://api.airbyte.com/v1/applications/token"
        payload = {
            "client_id": client_id,
            "client_secret": client_secret,
        }
        headers = {
            "content-type": "application/json",
            "accept": "application/json"
        }
        
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()  # Esto lanzará una excepción para códigos de estado 4xx/5xx
        
        response_data = response.json()
        new_token = response_data.get("access_token")
        
        if not new_token:
            logger.error(f"No token found in response: {response_data}")
            raise ValueError("No token found in response.")
        
        logger.info(f"New token: {new_token}")
        return new_token

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

@dag(
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
)
def data_to_azure_blob_airbyte_dbt():
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

    token = get_new_token()

    trigger_sync = SimpleHttpOperator(
        method="POST",
        task_id='start_airbyte_sync',
        http_conn_id='airbyte-api-cloud-connection',
        headers={
            "Content-Type": "application/json",
            "User-Agent": "fake-useragent",  # Airbyte cloud requires that a user agent is defined
            "Accept": "application/json",
            "Authorization": "Bearer {{ task_instance.xcom_pull(task_ids='get_new_token') }}"
        },
        endpoint='/v1/jobs',
        data=json.dumps({"connectionId": AIRBYTE_CONNECTION_ID, "jobType": "sync"}),
        do_xcom_push=True,
        response_filter=lambda response: response.json()['jobId'],
        log_response=True,
    )

    def check_sync_status(ti):
        job_id = ti.xcom_pull(task_ids='start_airbyte_sync')
        max_retries = 40
        retries = 0
        
        while retries < max_retries:
            token = ti.xcom_pull(task_ids='get_new_token')
            url = f"https://api.airbyte.com/v1/jobs/{job_id}"
            headers = {
                "Content-Type": "application/json",
                "User-Agent": "fake-useragent", 
                "Accept": "application/json",
                "Authorization": f"Bearer {token}"
            }
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            status = response.json().get('status')
            
            if status == 'succeeded':
                return True
            elif status == 'failed':
                raise ValueError("Airbyte sync failed.")
            
            retries += 1
            time.sleep(180)  # Wait for 3 minutes before retrying

        raise TimeoutError("Airbyte sync did not complete within the allowed time.")

    wait_for_sync_to_complete = PythonOperator(
        task_id='wait_for_airbyte_sync',
        python_callable=check_sync_status,
    )

    end = DummyOperator(task_id="end")

    t0 >> data_group >> token >> trigger_sync >> wait_for_sync_to_complete >> end

data_to_azure_blob_airbyte_dbt()
