from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import os

def upload_to_azure_blob(file_path: str, filename: str, folder_path: str) -> None:
    azurehook = WasbHook(wasb_conn_id="azure_blob")
    container_name = "intermediate"
    
    # Read the content of the CSV file
    with open(file_path, 'r') as file:
        data = file.read()
    
    # Determine the full path for the blob
    blob_name = f"{folder_path}/{filename}"
    
    # Upload the content to Azure Blob Storage
    azurehook.load_string(string_data=data, container_name=container_name, blob_name=blob_name)
