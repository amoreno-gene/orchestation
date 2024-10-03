from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator


AIRBYTE_CONN_ID='airbyte_azure_sync',
CONNECTION_ID='7cf90c02-27ca-4a38-911f-7c0e09d4eb2c'



def trigger_airbyte_sync(task_id: str):
    return AirbyteTriggerSyncOperator(
        task_id=task_id,
        airbyte_conn_id=AIRBYTE_CONN_ID,
        connection_id=CONNECTION_ID
    )