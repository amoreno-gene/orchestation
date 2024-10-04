from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.models import Variable  # Importar para obtener la variable

# Definir el DAG
dag = DAG(
    'test_dbt_cloud_connection_with_variable',
    start_date=days_ago(1),
    schedule_interval=None,  # Ejecuta el DAG manualmente
    catchup=False,
)

# Obtener el Job ID desde la variable de Airflow
dbt_job_id = Variable.get('dbt_test_job')

# Tarea para ejecutar un trabajo de dbt Cloud
test_dbt_connection = DbtCloudRunJobOperator(
    task_id='run_dbt_job',
    dbt_cloud_conn_id='dbt_cloud_conn',  # Usa el Connection ID que has configurado
    job_id=dbt_job_id,  # Utilizar la variable que contiene el Job ID
    dag=dag
)

# Definir el flujo de tareas
test_dbt_connection
