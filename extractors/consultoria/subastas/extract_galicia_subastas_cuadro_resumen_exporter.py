import requests
from bs4 import BeautifulSoup
import pandas as pd
from pandas_ods_reader import read_ods
import re
import os
from datetime import datetime

def extract_and_process_data():
    # URL de la página principal de subastas
    url = "https://mediorural.xunta.gal/es/temas/forestal/subastas"

    # Realizar la solicitud HTTP a la página
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Encontrar todos los enlaces que contienen "cuadro resumo", "cadro resumo" o "cuadro resumen" y terminan en .ods, .xlsx o .xls
    links = soup.find_all('a', href=True)
    cadro_links = [link['href'] for link in links if any(term in link.text.lower() for term in ["cuadro resumo", "cadro resumo", "cuadro resumen"]) and link['href'].lower().endswith(('.ods', '.xlsx', '.xls'))]

    print(cadro_links)

    # Crear un directorio para almacenar los archivos descargados
    if not os.path.exists('cadro_resumo_files'):
        os.makedirs('cadro_resumo_files')

    # Función para extraer año y provincia de la URL
    def extract_year_province(url):
        match = re.search(r'/([a-z]+)/(\d{4})/', url)
        if match:
            province, year = match.groups()
            return province, year
        return None, None

    # Crear un DataFrame vacío para almacenar los resultados
    final_df = pd.DataFrame()

    # Definir un diccionario de mapeo de columnas para unificar nombres
    column_mapping = {
        'nº_lote': 'nº_lote',
        'cert._pefc': 'cert._pefc',
        'cert._fsc': 'cert._fsc',
        'monte': 'monte',
        'propietario': 'propietario',
        'concello': 'concello',
        'motivo': 'motivo',
        'situación': 'situación',
        'especie': 'especie',
        'nº_pes': 'nº_pes',
        'sup._lote_ha': 'sup._lote_ha',
        'sup._loteha': 'sup._lote_ha',
        'ud': 'ud',
        '€/ud': '€/ud',
        '€/ud*': '€/ud',
        'taxación_€': 'taxación_€',
        'taxación€': 'taxación_€',
        'taxación': 'taxación_€',
        'taxas': 'taxas',
        'garantía_prov': 'garantía_prov',
        'garantía_prov.': 'garantía_prov',
        'prazo_meses': 'prazo_meses',
        'observacións': 'observacións',
        'observacións_*': 'observacións',
        'observacións*': 'observacións',
        'carbono_absorbido': 'carbono_absorbido',
        'carbono_absorbido_t': 'carbono_absorbido',
        'unidades': 'unidades',
        'tabla': 'tabla',
        'provincia': 'provincia',
        'año': 'año',
        'cert._une': 'cert._une',
        'cert_une': 'cert._une',
        'propiedade': 'propiedad',
        '100%_pefc_certificado': 'pefc_certificado',
        'fsc_100%': 'fsc_certificado',
        'lote': 'nº_lote',
        'cousa_certa': 'cousa_certa',
        'motivo_**': 'motivo_**',
        'nº__pés': 'nº_pes',
        'nº_pés': 'nº_pes',
        'Nº  pés': 'nº_pes',
        'Nª pés': 'nº_pes',
        'fianza_prov._*': 'fianza_prov',
        'obs._**': 'obs._**'
    }

    # Descargar cada archivo y procesarlo
    for link in cadro_links:
        file_url = link if link.startswith('http') else 'https://mediorural.xunta.gal' + link
        file_name = file_url.split('/')[-1]
        response = requests.get(file_url)
        
        file_path = f'cadro_resumo_files/{file_name}'
        with open(file_path, "wb") as file:
            file.write(response.content)
        
        print(f'Descargado: {file_name}')
        
        try:
            # Leer el archivo .ods, .xlsx o .xls utilizando pandas
            print(f'Intentando leer: {file_name}')
            if file_name.endswith('.ods'):
                df = read_ods(file_path, 1)
            else:
                df = pd.read_excel(file_path, sheet_name=0)
            print(f'Leído correctamente: {file_name}')
        except Exception as e:
            print(f'Error leyendo {file_name}: {e}')
            continue

        # Verificar si la primera fila contiene valores None
        if df.iloc[0].isnull().any():
            df.columns = df.iloc[1]  # La fila 2 se convierte en la cabecera
            df = df.drop([0, 1]).reset_index(drop=True)  # Eliminar las filas 0 y 1 y resetear el índice
        else:
            df.columns = df.iloc[0]  # La fila 1 se convierte en la cabecera
            df = df.drop([0]).reset_index(drop=True)  # Eliminar la fila 0 y resetear el índice

        # Asegurarse de que las columnas sean únicas
        df.columns = pd.Series(df.columns).str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
        df.columns = [f"{col}_{i}" if list(df.columns).count(col) > 1 else col for i, col in enumerate(df.columns)]

        # Renombrar las columnas usando el diccionario de mapeo
        df.rename(columns=column_mapping, inplace=True)

        # Renombrar las columnas m3 y €/m3 a ud y €/ud utilizando expresiones regulares
        df.columns = [re.sub(r'm3(\s*\(\*\)\s*)?', 'ud', col) for col in df.columns]
        df.columns = [re.sub(r'€/m3(\s*\(\*\)\s*)?', '€/ud', col) for col in df.columns]

        # Crear un DataFrame para almacenar los resultados
        result_df = pd.DataFrame(columns=df.columns)

        # Procesar cada fila para separar celdas unidas
        for _, row in df.iterrows():
            # Identificar si la fila tiene múltiples valores en alguna columna
            if isinstance(row[df.columns[8]], str) and '\n' in row[df.columns[8]]:
                species = row[df.columns[8]].split('\n')
                rest_values = row[df.columns[9:]].values

                for spec in species:
                    new_row = row.copy()
                    new_row[df.columns[8]] = spec.strip()
                    new_row[df.columns[9:]] = rest_values
                    result_df = pd.concat([result_df, pd.DataFrame([new_row])], ignore_index=True)
            else:
                result_df = pd.concat([result_df, pd.DataFrame([row])], ignore_index=True)

        # Rellenar los valores None con los valores de la fila superior
        result_df = result_df.ffill()

        # Filtrar filas que no siguen el patrón "Nº lote" o "XXX/YY"
        pattern = r"^(nº_lote|\d{3}/\d{2})$"
        result_df = result_df[result_df[result_df.columns[0]].astype(str).str.match(pattern)]

        # Renombrar la columna 'nº_pés' y agregar una columna de unidades
        if 'nº_pes' in result_df.columns:
            result_df.rename(columns={'nº_pés': 'nº_pes'}, inplace=True)

            # Definir unidades según 'nº_pes' y el contexto de las tablas
            result_df['unidades'] = result_df.apply(lambda x: 'tn' if pd.isna(x['nº_pes']) or x['nº_pes'] in ['-', '', 'Nº  pés'] else 'm3', axis=1)
        else:
            result_df['unidades'] = 'unknown'

        # Agregar la columna 'tabla' basada en las unidades
        result_df['tabla'] = result_df['unidades'].apply(lambda x: 'Lotes a Resultas' if x == 'tn' else ('Lotes a Risco e Ventura' if x == 'm3' else 'Desconocido'))

        # Extraer el año y la provincia de la URL y agregarlas como columnas
        province, year = extract_year_province(file_url)
        result_df['provincia'] = province
        result_df['año'] = year

        # Añadir la columna 'archivo' con el nombre del archivo
        result_df['archivo'] = file_name

        # Añadir los datos extraídos al DataFrame final
        final_df = pd.concat([final_df, result_df], ignore_index=True)

    # Eliminar las columnas que no contengan ningún dato
    final_df.dropna(axis=1, how='all', inplace=True)

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"subastas_galicia_cuadro_resumen_{timestamp}.csv"
    # Guardar el resultado en un archivo CSV
    final_df.to_csv(filename, index=False)

    return filename
