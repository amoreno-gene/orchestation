import eurostat
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import os

# Configurar logging para ver las extracciones
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Función que obtiene la lista de países disponibles para 'reporter' desde Eurostat
def get_available_countries(dataset_code):
    try:
        countries = eurostat.get_par_values(dataset_code, 'reporter')
        logger.info(f"Países disponibles: {countries}")
        return countries
    except Exception as e:
        logger.error(f"Error al obtener los países disponibles: {e}")
        return []

# Función que obtiene la lista de productos disponibles desde Eurostat
def get_available_products(dataset_code):
    try:
        products = eurostat.get_par_values(dataset_code, 'product')
        logger.info(f"Productos disponibles: {products}")
        return products
    except Exception as e:
        logger.error(f"Error al obtener los productos disponibles: {e}")
        return []

# Función que realiza la extracción de datos para un país, producto y año específico
def extract_data_for_product_country_year(product, country, year, dataset_code, common_filters):
    my_filter_pars = common_filters.copy()
    my_filter_pars['time_period'] = [str(year)]  # Filtrar por año específico
    my_filter_pars['reporter'] = [country]  # Filtrar por país que reporta (reporter)
    my_filter_pars['product'] = [product]  # Filtrar por producto
    
    try:
        logger.info(f"Descargando datos para el producto {product}, el año {year} y el país {country}...")
        data = eurostat.get_data_df(dataset_code, filter_pars=my_filter_pars)
        
        # Despivoteo del dataframe
        melted_data = data.melt(id_vars=['freq', 'reporter', 'partner', 'product', 'flow', 'indicators\\TIME_PERIOD'], 
                                var_name='DATE', value_name='VALUE')

        logger.info(f"Datos descargados y procesados para el producto {product}, el año {year} y el país {country}")
        return melted_data

    except Exception as e:
        logger.error(f"Error al descargar datos para el producto {product}, el año {year} y el país {country}: {e}")
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error

# Función principal que maneja la extracción en paralelo, guardando los resultados incrementalmente
def extract_and_process_data():
    dataset_code = 'DS-045409'  # Código del dataset en Eurostat
    
    # Generar un rango de años desde 2020 hasta el año actual
    years = list(range(2020, datetime.now().year + 1))

    # Obtener la lista de todos los países y productos disponibles desde Eurostat
    countries = get_available_countries(dataset_code)
    products = get_available_products(dataset_code)

    common_filters = {
        'freq': ['M'],  # Solo datos mensuales
    }

    if not countries or not products:
        logger.error("No se pudieron obtener los países o productos disponibles.")
        return None

    # Definir el archivo CSV final
    final_csv_filename = f"eurostat_data_combined_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"

    # Variable para saber si ya se ha escrito el encabezado
    header_written = False

    # Crear un pool de threads para ejecutar las extracciones en paralelo
    with ThreadPoolExecutor(max_workers=5) as executor:  # Ajusta el número de hilos según sea necesario
        # Dividir la extracción por producto, país y año
        future_to_product_country_year = {
            executor.submit(extract_data_for_product_country_year, product, country, year, dataset_code, common_filters): (product, country, year)
            for product in products for country in countries for year in years
        }

        # Procesar los resultados a medida que se completen
        for future in as_completed(future_to_product_country_year):
            product, country, year = future_to_product_country_year[future]
            try:
                data = future.result()
                if not data.empty:
                    # Guardar los datos en el CSV de forma incremental
                    mode = 'a' if header_written else 'w'
                    data.to_csv(final_csv_filename, mode=mode, header=not header_written, index=False)
                    header_written = True  # Asegurar que el encabezado solo se escriba una vez
                    logger.info(f"Datos del producto {product}, país {country} y año {year} guardados en {final_csv_filename}")
            except Exception as exc:
                logger.error(f"Producto {product}, país {country} para el año {year} generó una excepción: {exc}")

    logger.info(f"Extracción completada. Archivo final: {final_csv_filename}")
    return final_csv_filename  # Retornar la ruta del archivo CSV generado

# Ejecución del script de extracción
if __name__ == "__main__":
    try:
        final_csv_file = extract_and_process_data()
        if final_csv_file:
            logger.info(f"Extracción completada. Archivo generado: {final_csv_file}")
        else:
            logger.error("No se generó ningún archivo debido a un fallo en la extracción.")
    except Exception as e:
        logger.error(f"Error general en el proceso de extracción: {e}")
