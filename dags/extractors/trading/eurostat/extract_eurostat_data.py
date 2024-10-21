import eurostat
import pandas as pd
import logging
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

# Función que realiza la extracción de datos para un país, producto y año específico
def extract_data_for_product_country_year(product, country, year, dataset_code, common_filters):
    my_filter_pars = common_filters.copy()
    my_filter_pars['time_period'] = [str(year)]  # Filtrar por año específico
    my_filter_pars['reporter'] = [country]  # Filtrar por país que reporta (reporter)
    my_filter_pars['product'] = [product]  # Filtrar por producto específico
    
    try:
        logger.info(f"Descargando datos para el producto {product}, el año {year} y el país {country}...")
        data = eurostat.get_data_df(dataset_code, filter_pars=my_filter_pars)
        
        # Despivoteo del dataframe
        melted_data = data.melt(id_vars=['freq', 'reporter', 'partner', 'product', 'flow', 'indicators\TIME_PERIOD'], 
                                var_name='DATE', value_name='VALUE')

        # Reestructurar el dataframe con las columnas deseadas
        pivoted_data = melted_data.pivot_table(index=['DATE', 'reporter', 'partner', 'product'], 
                                               columns='indicators\TIME_PERIOD', values='VALUE', 
                                               aggfunc='first').reset_index()

        # Renombrar las columnas
        pivoted_data.columns.name = None
        pivoted_data = pivoted_data.rename(columns={
            'QUANTITY_IN_100KG': 'QUANTITY_IN_100KG',
            'SUPPLEMENTARY_QUANTITY': 'SUPPLEMENTARY_QUANTITY',
            'VALUE_IN_EUROS': 'VALUE_IN_EUROS'
        })

        logger.info(f"Datos descargados y procesados para el producto {product}, el año {year} y el país {country}")
        return pivoted_data

    except Exception as e:
        logger.error(f"Error al descargar datos para el producto {product}, el año {year} y el país {country}: {e}")
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error

# Función principal que maneja la extracción en secuencia, guardando los resultados incrementalmente
def extract_and_process_data():
    dataset_code = 'DS-045409'  # Código del dataset en Eurostat
    
    # Generar un rango de años desde 2020 hasta el año actual
    years = list(range(2020, datetime.now().year + 1))

    # Obtener la lista de todos los países disponibles desde Eurostat
    countries = get_available_countries(dataset_code)
    
    # Filtrar solo los productos específicos que deseas extraer
    products = ['44012100', '44012210', '44012290', '44013100', '44039800']  # Lista de productos a extraer

    common_filters = {
        'freq': ['M'],  # Solo datos mensuales
    }

    if not countries or not products:
        logger.error("No se pudieron obtener los países o productos disponibles.")
        return []

    # Crear una lista para almacenar los datos descargados
    df_list = []

    # Iterar sobre cada combinación de producto, país y año
    for product in products:
        for country in countries:
            for year in years:
                try:
                    data = extract_data_for_product_country_year(product, country, year, dataset_code, common_filters)
                    if not data.empty:
                        # Añadir los datos descargados a la lista
                        df_list.append(data)
                        logger.info(f"Datos del producto {product}, país {country} y año {year} procesados.")
                except Exception as exc:
                    logger.error(f"Producto {product}, país {country} para el año {year} generó una excepción: {exc}")

    # Combinar todos los dataframes descargados en uno solo
    if df_list:
        final_df = pd.concat(df_list, ignore_index=True)

        # Guardar los datos reestructurados en un archivo CSV
        final_csv_filename = f"eurostat_data_combined_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        final_df.to_csv(final_csv_filename, index=False)

        logger.info(f"Extracción completada. Archivo final: {final_csv_filename}")
        return [final_csv_filename]  # Retornar una lista con la ruta del archivo CSV generado
    else:
        logger.error("No se obtuvieron datos para los productos y países especificados.")
        return []

# Ejecución del script de extracción
if __name__ == "__main__":
    try:
        final_csv_files = extract_and_process_data()  # Ahora retorna una lista
        if final_csv_files:
            logger.info(f"Extracción completada. Archivos generados: {final_csv_files}")
        else:
            logger.error("No se generó ningún archivo debido a un fallo en la extracción.")
    except Exception as e:
        logger.error(f"Error general en el proceso de extracción: {e}")
