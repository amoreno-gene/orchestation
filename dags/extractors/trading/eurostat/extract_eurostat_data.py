import eurostat
import pandas as pd
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import os

# Configurar logging para ver las extracciones
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Consulta SQL para obtener la fecha máxima de datos en Snowflake
SNOWFLAKE_FROM_QUERY = "SELECT MAX(DATE) FROM GENERANDIDEVDB.SH_STG.EUROSTAT_DATA;"


def get_active_origins():
    logger.info("Iniciando la función get_active_origins")
    hook = SnowflakeHook(snowflake_conn_id='Snowflake_stg_schema_conn')  # Conexión configurada en Google Cloud
    conn = hook.get_conn()
    cur = conn.cursor()
    logger.info("Consultando fecha máxima desde Snowflake...")
    cur.execute(SNOWFLAKE_FROM_QUERY)
    result = cur.fetchone()
    cur.close()
    
    if result and result[0]:
        # Convertir max_date a un objeto datetime si es una cadena
        max_date = result[0]
        if isinstance(max_date, str):
            max_date = datetime.strptime(max_date, '%Y-%m-%d')  # Ajusta el formato según el formato de la fecha en Snowflake
        
        # Calcular la fecha de inicio restando dos meses
        start_date = max_date - relativedelta(months=2)
        logger.info(f"Fecha máxima en Snowflake: {max_date}, extrayendo desde: {start_date}")
        return start_date
    else:
        logger.warning("No se encontró una fecha máxima en Snowflake. Se usará una fecha por defecto.")
        return datetime(2020, 1, 1)  # Fecha de inicio por defecto


# Función que obtiene la lista de valores disponibles para un parámetro desde Eurostat
def get_available_values(dataset_code, parameter):
    try:
        values = eurostat.get_par_values(dataset_code, parameter)
        logger.info(f"Valores disponibles para {parameter}: {values}")
        return values
    except Exception as e:
        logger.error(f"Error al obtener los valores disponibles para {parameter}: {e}")
        return []

# Función que obtiene la lista de países disponibles para 'reporter' desde Eurostat
def get_available_countries(dataset_code):
    return get_available_values(dataset_code, 'reporter')

# Función que realiza la extracción de datos para un país, producto y año específico
def extract_data_for_product_country_year(product, country, year, flow, dataset_code, common_filters):
    my_filter_pars = common_filters.copy()
    my_filter_pars['time_period'] = [str(year)]  # Filtrar por año específico
    my_filter_pars['reporter'] = [country]  # Filtrar por país que reporta (reporter)
    my_filter_pars['product'] = [product]  # Filtrar por producto específico
    my_filter_pars['flow'] = [flow]  # Filtrar por flujo específico
    
    try:
        logger.info(f"Descargando datos para el producto {product}, el año {year}, el flujo {flow} y el país {country}...")
        data = eurostat.get_data_df(dataset_code, filter_pars=my_filter_pars)
        
        # Despivoteo del dataframe
        melted_data = data.melt(id_vars=['freq', 'reporter', 'partner', 'product', 'flow', 'indicators\\TIME_PERIOD'], 
                                var_name='DATE', value_name='VALUE')

        # Reestructurar el dataframe con las columnas deseadas
        pivoted_data = melted_data.pivot_table(index=['DATE', 'reporter', 'partner', 'product', 'flow'], 
                                               columns='indicators\\TIME_PERIOD', values='VALUE', 
                                               aggfunc='first').reset_index()

        # Renombrar las columnas
        pivoted_data.columns.name = None
        pivoted_data = pivoted_data.rename(columns={
            'QUANTITY_IN_100KG': 'QUANTITY_IN_100KG',
            'SUPPLEMENTARY_QUANTITY': 'SUPPLEMENTARY_QUANTITY',
            'VALUE_IN_EUROS': 'VALUE_IN_EUROS'
        })

        # Asegurarse de que las columnas 'freq' y 'flow' estén presentes
        if 'freq' not in pivoted_data.columns:
            pivoted_data['FREQ'] = my_filter_pars.get('freq', ['M'])[0]  # Añadir frecuencia por defecto si no existe

        logger.info(f"Datos descargados y procesados para el producto {product}, el año {year}, el flujo {flow} y el país {country}")
        return pivoted_data

    except Exception as e:
        logger.error(f"Error al descargar datos para el producto {product}, el año {year}, el flujo {flow} y el país {country}: {e}")
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error

def extract_and_process_data():
    # Obtener la fecha de inicio llamando a get_active_origins
    start_date = get_active_origins()
    logger.info(f"Fecha de inicio de extracción obtenida: {start_date}")

    dataset_code = 'DS-045409'  # Código del dataset en Eurostat
    
    # Generar un rango de años desde la fecha de inicio calculada hasta el año actual
    years = list(range(start_date.year, datetime.now().year + 1))

    # Obtener la lista de todos los países disponibles desde Eurostat
    countries = get_available_countries(dataset_code)
    
    # Filtrar solo los productos específicos que deseas extraer
    products = ['44012100', '44012210', '44012290', '44013100', '44039800']  # Lista de productos a extraer

    # Obtener todos los flujos disponibles
    flows = get_available_values(dataset_code, 'flow')

    common_filters = {
        'freq': ['M'],  # Solo datos mensuales
    }

    if not countries or not products or not flows:
        logger.error("No se pudieron obtener los países, productos o flujos disponibles.")
        return []

    # Crear una lista para almacenar los datos descargados
    df_list = []

    # Iterar sobre cada combinación de producto, país, año y flujo
    for product in products:
        for country in countries:
            for year in years:
                if year < start_date.year:
                    continue  # Saltar años anteriores a start_date
                
                for flow in flows:
                    try:
                        data = extract_data_for_product_country_year(product, country, year, flow, dataset_code, common_filters)
                        if not data.empty:
                            # Filtrar los datos obtenidos por la fecha exacta en caso de que start_date esté en el mismo año
                            data = data[data['DATE'] >= start_date.strftime('%Y-%m-%d')]
                            df_list.append(data)
                            logger.info(f"Datos del producto {product}, país {country}, año {year} y flujo {flow} procesados.")
                    except Exception as exc:
                        logger.error(f"Producto {product}, país {country}, año {year} y flujo {flow} generó una excepción: {exc}")

    # Combinar todos los dataframes descargados en uno solo
    if df_list:
        final_df = pd.concat(df_list, ignore_index=True)

        # Guardar los datos reestructurados en un archivo CSV
        final_csv_filename = f"eurostat_data_combined_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        final_df.to_csv(final_csv_filename, index=False)

        logger.info(f"Extracción completada. Archivo final: {final_csv_filename}")
        return [final_csv_filename]  # Retornar una lista con la ruta del archivo CSV generado
    else:
        logger.error("No se obtuvieron datos para los productos, países y flujos especificados.")
        return []

# Ejecución del script de extracción
if __name__ == "__main__":
    try:
        start_date = get_active_origins()
        logger.info(f"Fecha obtenida de Snowflake: {start_date}")
        final_csv_files = extract_and_process_data(start_date)  # Pasa `start_date` calculado
        if final_csv_files:
            logger.info(f"Extracción completada. Archivos generados: {final_csv_files}")
        else:
            logger.error("No se generó ningún archivo debido a un fallo en la extracción.")
    except Exception as e:
        logger.error(f"Error general en el proceso de extracción: {e}")
