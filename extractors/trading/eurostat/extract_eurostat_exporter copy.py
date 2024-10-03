import eurostat
import pandas as pd

# Código del dataset
dataset_code = 'DS-045409'

# Obtener los posibles valores para las dimensiones del dataset
pars = eurostat.get_pars(dataset_code)
print(f"Dimensiones disponibles: {pars}")

# Usar get_par_values para obtener los valores de la dimensión 'product'
products = eurostat.get_par_values(dataset_code, 'product')
print(f"Productos disponibles: {products}")

# Filtrar los productos que comienzan con '44' y tienen 8 dígitos
filtered_products = [prod for prod in products if prod.startswith('44') and len(prod) == 8]
print(f"Productos filtrados: {filtered_products}")

# Crear una lista para almacenar los datos descargados
df_list = []

# Definir los filtros comunes para todas las peticiones
common_filters = {
    'flow': ['1'],  # Valor 1 corresponde a IMPORT
    'freq': ['M'],  # Solo datos mensuales
    'partner': ['WORLD'],  # Solo datos con el socio 'WORLD'
}

# Realizar la solicitud para cada producto filtrado
for product in filtered_products:
    my_filter_pars = common_filters.copy()
    my_filter_pars['product'] = [product]  # Filtrar por cada producto específico
    
    try:
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

        # Añadir los datos descargados a la lista
        df_list.append(pivoted_data)

    except Exception as e:
        print(f"Error al descargar datos para el producto {product}: {e}")

# Combinar todos los dataframes descargados en uno solo
if df_list:
    final_df = pd.concat(df_list, ignore_index=True)

    # Guardar los datos reestructurados en un archivo CSV
    output_path = 'filtered_and_despivoted_data.csv'
    final_df.to_csv(output_path, index=False)

    # Mostrar mensaje de éxito
    print(f"Los datos filtrados y reestructurados se han guardado en '{output_path}'.")
else:
    print("No se obtuvieron datos para los productos filtrados.")
