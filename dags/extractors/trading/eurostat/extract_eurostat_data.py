# extract_eurostat_data.py

import eurostat
import pandas as pd
import concurrent.futures

# Código del dataset
dataset_code = 'DS-045409'

# Filtros comunes
common_filters = {
    'flow': ['1'],
    'freq': ['M'],
    'partner': ['WORLD']
}

def extract_and_process_data():
    # Usar get_par_values para obtener los valores de la dimensión 'product'
    products = eurostat.get_par_values(dataset_code, 'product')

    # Filtrar los productos que comienzan con '44' y tienen 8 dígitos
    filtered_products = [prod for prod in products if prod.startswith('44') and len(prod) == 8]

    def download_data_for_product(product):
        my_filter_pars = common_filters.copy()
        my_filter_pars['product'] = [product]

        try:
            #
            # Descargar los datos para el producto específico
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

            return pivoted_data

        except Exception as e:
            print(f"Error al descargar datos para el producto {product}: {e}")
            return pd.DataFrame()  # Retornar un DataFrame vacío en caso de error

    # Usar ThreadPoolExecutor para descargar datos de forma concurrente
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(download_data_for_product, filtered_products))

    # Combinar todos los DataFrames descargados en uno solo
    final_df = pd.concat(results, ignore_index=True)

    # Guardar los datos en un archivo CSV
    output_path = '/home/airflow/gcs/data/eurostat/filtered_and_despivoted_data.csv'
    final_df.to_csv(output_path, index=False)

    return output_path  # Retornar la ruta del archivo CSV generado
