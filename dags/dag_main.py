from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import os
import logging
from datetime import datetime as dt
from uploaders.upload_to_gcs import upload_to_gcs  # Importar la función desde tu archivo

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Definir parámetros
SNOWFLAKE_CONN_ID = 'Snowflake_orchestation_metadata_schema_conn'
GCS_BUCKET_NAME = "intermediate-datalake"
ORQUESTADOR_ID = 1  

# Consulta SQL para obtener los orígenes activos
SNOWFLAKE_ORIGINS_QUERY = f"""
    SELECT 
        o.id_origen, o.nombre_origen, cu.nombre_caso AS nombre_caso_uso, cu.area_negocio
    FROM    origenes_orquestadores oo
        JOIN origenes o ON oo.id_origen = o.id_origen
        JOIN casos_uso cu ON o.id_caso_uso = cu.id_caso_uso
    WHERE 
        oo.id_orquestador = {ORQUESTADOR_ID}
        AND o.activo = TRUE
        AND cu.activo = TRUE;

"""

# Función para importar dinámicamente un módulo de un archivo
def import_module_from_path(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# Definir DAG
@dag(
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
)
def dag_main_orquestador_uno():
    t0 = DummyOperator(task_id="start")

    # Tarea para consultar orígenes activos y el caso de uso en Snowflake
    @task(task_id="get_active_origins")
    def get_active_origins():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        logger.info("Consultando orígenes activos desde Snowflake...")
        cur.execute(SNOWFLAKE_ORIGINS_QUERY)
        result = cur.fetchall()
        cur.close()
        logger.info(f"Orígenes obtenidos: {result}")
        return result

    @task(task_id="extract_and_upload_dynamic", multiple_outputs=True)
    def extract_and_upload_dynamic(origins):
        for origin in origins:
            id_origen, nombre_origen, nombre_caso_uso, area_negocio = origin
            logger.info(f"Extrayendo datos para el origen: {nombre_origen} (ID: {id_origen}), Caso de uso: {nombre_caso_uso}, Área de negocio: {area_negocio}")

            # Ruta del script de extracción correspondiente dentro de la carpeta del área de negocio y caso de uso
            extractor_script = f"extract_{nombre_origen.lower()}.py"
            extractor_path = os.path.join(f"extractors/{area_negocio}/{nombre_caso_uso}/", extractor_script)

            # Verificar si el script existe
            if os.path.exists(extractor_path):
                module = import_module_from_path(nombre_origen.lower(), extractor_path)
                csv_file = module.extract_and_process_data()  # Llamada a la función de extracción
                timestamp = dt.now().strftime("%Y%m%d%H%M%S")
                filename = f"{os.path.splitext(os.path.basename(csv_file))[0]}_{timestamp}.csv"
                folder_path = f"{area_negocio}/orquestador_{ORQUESTADOR_ID}/origen_{id_origen}/caso_uso_{nombre_caso_uso}"
                upload_to_gcs(csv_file, filename, folder_path)  # Subida del archivo a GCS
                logger.info(f"Datos extraídos y subidos para {nombre_origen}, Caso de uso {nombre_caso_uso} en área {area_negocio}")
            else:
                logger.error(f"No se encontró el script de extracción para {nombre_origen} en el caso de uso {nombre_caso_uso} y área de negocio {area_negocio}")


    # Definir flujo de tareas
    origins = get_active_origins()
    extract_and_upload_dynamic(origins)

    t0 >> origins

dag_main_orquestador_uno()