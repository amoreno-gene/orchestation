import eurostat
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Configurar logging para ver las extracciones
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Función que realiza la extracción de datos para un producto y año específico
def extract_product_data(product, year, dataset_code, common_filters):
    my_filter_pars = common_filters.copy()
    my_filter_pars['product'] = [product]  # Filtrar por cada producto específico
    my_filter_pars['time_period'] = [str(year)]  # Filtrar por año específico
    
    try:
        logger.info(f"Descargando datos para el producto {product} en el año {year}...")
        data = eurostat.get_data_df(dataset_code, filter_pars=my_filter_pars)
        
        # Despivoteo del dataframe
        melted_data = data.melt(id_vars=['freq', 'reporter', 'partner', 'product', 'flow', 'indicators\\TIME_PERIOD'], 
                                var_name='DATE', value_name='VALUE')

        # Reestructurar el dataframe con las columnas deseadas
        pivoted_data = melted_data.pivot_table(index=['DATE', 'reporter', 'partner', 'product'], 
                                               columns='indicators\\TIME_PERIOD', values='VALUE', 
                                               aggfunc='first').reset_index()

        # Renombrar las columnas
        pivoted_data.columns.name = None
        pivoted_data = pivoted_data.rename(columns={
            'QUANTITY_IN_100KG': 'QUANTITY_IN_100KG',
            'SUPPLEMENTARY_QUANTITY': 'SUPPLEMENTARY_QUANTITY',
            'VALUE_IN_EUROS': 'VALUE_IN_EUROS'
        })

        logger.info(f"Datos descargados y procesados para el producto {product} en el año {year}")
        return pivoted_data

    except Exception as e:
        logger.error(f"Error al descargar datos para el producto {product} en el año {year}: {e}")
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error

# Función principal que maneja la extracción en paralelo, año por año
def extract_and_process_data():
    dataset_code = 'DS-045409'  # Código del dataset en Eurostat
    products = ['44012100', '44012210', '44012290', '44013100', '44039800']  # Lista de productos a extraer
    
    # Generar un rango de años desde 2020 hasta el año actual
    years = list(range(2020, datetime.now().year + 1))

    common_filters = {
        'freq': ['M'],  # Solo datos mensuales
    }

    df_list = []

    # Crear un pool de threads para ejecutar las extracciones en paralelo
    with ThreadPoolExecutor(max_workers=len(products)) as executor:
        # Dividir la extracción por producto y año
        future_to_product_year = {
            executor.submit(extract_product_data, product, year, dataset_code, common_filters): (product, year)
            for product in products for year in years
        }

        for future in as_completed(future_to_product_year):
            product, year = future_to_product_year[future]
            try:
                data = future.result()
                if not data.empty:
                    df_list.append((data, product, year))  # Guardar tanto los datos como el producto y el año
            except Exception as exc:
                logger.error(f"Producto {product} para el año {year} generó una excepción: {exc}")
    
    # Si no hay datos para ninguno de los productos
    if not df_list:
        logger.error("No se obtuvieron datos para los productos especificados.")
        raise Exception("Fallo en la extracción de datos de Eurostat.")

    # Guardar los datos en archivos CSV y retornar la lista de archivos generados
    csv_files = []  # Lista de archivos generados
    for df, product, year in df_list:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"eurostat_{product}_{year}_{timestamp}.csv"
        df.to_csv(filename, index=False)
        csv_files.append(filename)  # Añadir a la lista de archivos generados

    return csv_files  # Retornar la lista de archivos CSV

# Ejecución del script de extracción
if __name__ == "__main__":
    try:
        csv_files = extract_and_process_data()
        logger.info(f"Extracción completada. Archivos generados: {csv_files}")
    except Exception as e:
        logger.error(f"Error general en el proceso de extracción: {e}")
