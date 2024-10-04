# Google Cloud Storage Upload
from airflow.providers.google.cloud.hooks.gcs import GCSHook

def upload_to_gcs(file_path: str, filename: str, folder_path: str) -> None:
    gcs_hook = GCSHook()
    with open(file_path, 'r') as file:
        data = file.read()
    blob_name = f"{folder_path}/{filename}"
    gcs_hook.upload(bucket_name="intermediate-datalake", object_name=blob_name, data=data)
    print(f"Uploaded {file_path} to gs://intermediate-datalake/{blob_name}")
