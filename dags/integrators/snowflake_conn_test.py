from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable

# Definir los parámetros básicos del DAG
default_args = {
    'start_date': days_ago(1),
    'catchup': False
}

# DAG para probar la conexión a Snowflake
with DAG(
    dag_id='snowflake_test_connection',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    description='Test connection to Snowflake'
) as dag:

    # Operador para ejecutar una consulta SQL en Snowflake
    run_snowflake_query = SnowflakeOperator(
        task_id='run_snowflake_query',
        snowflake_conn_id='Snowflake_stg_schema_conn',  # Asegúrate de que esta conexión está configurada en Airflow
        sql="SELECT CURRENT_VERSION();"
    )

    run_snowflake_query

