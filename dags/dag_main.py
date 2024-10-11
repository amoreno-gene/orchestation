from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import os
import logging
import importlib
from datetime import datetime as dt
from uploaders.upload_to_gcs import upload_to_gcs  # Importar la función desde tu archivo

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Definir parámetros
SNOWFLAKE_CONN_ID = 'Snowflake_orchestation_metadata_schema_conn'  # Orquestador (metadatos)
SNOWFLAKE_STG_CONN_ID = 'Snowflake_stg_schema_conn'  # Para la conexión que apunta a SH_STG
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

# Función para crear o modificar tabla en Snowflake basándose en el esquema del CSV
def create_or_modify_table_from_csv(hook, table_name, csv_file):
    import csv
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        columns = next(reader)  # La primera fila contiene los nombres de las columnas
    
    conn = hook.get_conn()
    cur = conn.cursor()

    # Verificar si la tabla existe
    table_exists_query = f"SHOW TABLES LIKE '{table_name}';"
    cur.execute(table_exists_query)
    table_exists = cur.fetchone()

    if not table_exists:
        # Crear tabla si no existe
        columns_sql = ', '.join([f"{col} STRING" for col in columns])
        create_table_sql = f"CREATE TABLE {table_name} ({columns_sql});"
        logger.info(f"Creando tabla en SH_STG: {create_table_sql}")
        cur.execute(create_table_sql)
    else:
        # Si la tabla existe, verificar si faltan columnas y agregarlas
        describe_table_query = f"DESCRIBE TABLE {table_name};"
        cur.execute(describe_table_query)
        existing_columns = [row[0].upper() for row in cur.fetchall()]  # Obtener las columnas existentes en mayúsculas

        for col in columns:
            if col.upper() not in existing_columns:  # Comparar en mayúsculas
                alter_table_sql = f"ALTER TABLE {table_name} ADD COLUMN {col} STRING;"
                logger.info(f"Añadiendo columna: {alter_table_sql}")
                cur.execute(alter_table_sql)
            else:
                logger.info(f"La columna '{col}' ya existe en la tabla {table_name}, no se añadirá.")
    
    cur.close()

# Función para cargar archivos a Snowflake usando COPY INTO
def load_files_to_snowflake(hook, stage_name, table_name):
    conn = hook.get_conn()
    cur = conn.cursor()

    # Ejecutar COPY INTO, filtrando por los archivos no procesados
    copy_into_sql = f"""
    COPY INTO {table_name}
    FROM @{stage_name}
    FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
    FILES = (SELECT METADATA$FILENAME FROM @{stage_name}
             WHERE METADATA$FILENAME NOT IN (SELECT nombre_archivo FROM SH_CONTROL.archivos_procesados))
    ON_ERROR = 'CONTINUE';
    """
    try:
        cur.execute(copy_into_sql)
        logger.info(f"COPY INTO ejecutado para la tabla: {table_name}")
    except Exception as e:
        logger.error(f"Error al ejecutar COPY INTO para {table_name}: {str(e)}")

    cur.close()

def update_processed_files(hook, stage_name):
    conn = hook.get_conn()
    cur = conn.cursor()

    # Registrar los archivos cargados en la tabla SH_CONTROL.archivos_procesados
    insert_processed_sql = f"""
    INSERT INTO SH_CONTROL.archivos_procesados (nombre_archivo, fecha_procesado)
    SELECT DISTINCT METADATA$FILENAME, CURRENT_TIMESTAMP()
    FROM @{stage_name}
    WHERE METADATA$FILENAME NOT IN (SELECT nombre_archivo FROM SH_CONTROL.archivos_procesados);
    """
    try:
        cur.execute(insert_processed_sql)
        logger.info("Archivos procesados registrados en la tabla de control SH_CONTROL.archivos_procesados")
    except Exception as e:
        logger.error(f"Error al registrar archivos procesados: {str(e)}")
    
    cur.close()



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
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)  # Metadatos desde esta conexión
        conn = hook.get_conn()
        cur = conn.cursor()
        logger.info("Consultando orígenes activos desde Snowflake...")
        cur.execute(SNOWFLAKE_ORIGINS_QUERY)
        result = cur.fetchall()
        cur.close()
        logger.info(f"Orígenes obtenidos: {result}")
        return result

    # Tarea para extraer y subir datos de orígenes activos
    @task(task_id="extract_and_upload_dynamic", multiple_outputs=True)
    def extract_and_upload_dynamic(origins):
        uploaded_files = {}
        for origin in origins:
            id_origen, nombre_origen, nombre_caso_uso, area_negocio = origin
            logger.info(f"Extrayendo datos para el origen: {nombre_origen} (ID: {id_origen}), Caso de uso: {nombre_caso_uso}, Área de negocio: {area_negocio}")

            # Ruta del script de extracción correspondiente dentro de la carpeta del área de negocio y caso de uso
            extractor_script = f"extract_{nombre_origen.lower()}.py"
            extractor_path = os.path.join(f"/home/airflow/gcs/dags/extractors/{area_negocio}/{nombre_caso_uso}/", extractor_script)

            logger.info(f"Buscando script de extracción en la ruta: {extractor_path}")

            if os.path.exists(extractor_path):
                module = import_module_from_path(nombre_origen.lower(), extractor_path)
                csv_file = module.extract_and_process_data()  # Llamada a la función de extracción
                timestamp = dt.now().strftime("%Y%m%d%H%M%S")
                filename = f"{os.path.splitext(os.path.basename(csv_file))[0]}_{timestamp}.csv"
                folder_path = f"{area_negocio}/orquestador_{ORQUESTADOR_ID}/origen_{id_origen}/caso_uso_{nombre_caso_uso}"
                upload_to_gcs(csv_file, filename, folder_path)  # Subida del archivo a GCS
                logger.info(f"Datos extraídos y subidos para {nombre_origen}, Caso de uso {nombre_caso_uso} en área {area_negocio}")

                # Agregar el archivo subido al diccionario
                uploaded_files[nombre_origen] = f"{folder_path}/{filename}"

                # Crear tabla o modificarla según el esquema del CSV
                stg_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_STG_CONN_ID)  # Usamos la conexión de SH_STG
                create_or_modify_table_from_csv(stg_hook, nombre_origen, csv_file)
            else:
                logger.error(f"No se encontró el script de extracción en la ruta: {extractor_path} para el origen {nombre_origen} en el caso de uso {nombre_caso_uso} y área de negocio {area_negocio}")
    
        return uploaded_files  # Devolver un diccionario en lugar de una lista


    # Tarea para cargar archivos a Snowflake
    @task(task_id="load_to_snowflake")
    def load_to_snowflake(uploaded_files):
        stg_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_STG_CONN_ID)  # Cargar a SH_STG usando la conexión específica
        stage_name = 'my_gcs_stage'
        for nombre_origen, file in uploaded_files.items():
            table_name = nombre_origen  # Usar el nombre del origen para la tabla
            load_files_to_snowflake(stg_hook, stage_name, table_name)

        # Después de cargar los archivos, actualizar la tabla de control con los archivos procesados
        update_processed_files(stg_hook, stage_name)

    # Flujo del DAG
    origins = get_active_origins()
    uploaded_files = extract_and_upload_dynamic(origins)
    load_to_snowflake_task = load_to_snowflake(uploaded_files)

    # Definir la secuencia de tareas
    t0 >> origins >> uploaded_files >> load_to_snowflake_task

# Instancia del DAG
dag_main_orquestador_uno()

