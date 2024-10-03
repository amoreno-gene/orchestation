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
    cadro_links = [link['href'] for link in links if any(term in link.text.lower() for term in ["apertura"]) and link['href'].lower().endswith(('.ods', '.xlsx', '.xls'))]

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
        'id_lote': 'nº_lote',
        'lote':'nº_lote',
        'cert._pefc': 'cert._pefc',
        'cert._fsc': 'cert._fsc',
        'monte': 'monte',
        'propietario': 'propietario',
        'concello': 'concello',
        'motivo': 'motivo',
        'situación': 'situación',
        'especie': 'especie',
        'nº_pés': 'nº_pes',
        'sup._lote_ha': 'sup._lote_ha',
        'sup._lote':'sup._lote_ha',
        'sup._lote______ha': 'sup._lote_ha',  # Normalización de nombre de columna
        'ud': 'ud',
        '€/ud': '€/ud',
        '€/ud_*': '€/ud',  # Normalización de nombre de columna
        'taxación_€': 'taxación_€',
        'taxación€': 'taxación_€',
        'taxación': 'taxación_€',
        'taxas': 'taxas',
        'taxas_€': 'taxas',  # Normalización de nombre de columna
        'garantía_prov': 'garantía_prov',
        'garantía_prov.': 'garantía_prov',
        'fianza_prov': 'fianza_prov',  # Normalización de nombre de columna
        'prazo_meses': 'prazo_meses',
        'prazo____meses': 'prazo_meses',  # Normalización de nombre de columna
        'observacións': 'observacións',
        'observacións_*': 'observacións',
        'observacións*': 'observacións',
        'carbono_absorbido': 'carbono_absorbido',
        'archivo': 'archivo',
        'unidades': 'unidades',
        'tabla': 'tabla',
        'provincia': 'provincia',
        'año': 'año',
        'cert._une': 'cert._une',
        'cert_une': 'cert._une',  # Normalización de nombre de columna
        'propiedad': 'propiedad',
        'pefc_certificado': 'pefc_certificado',
        '100%_pefc_certificado': 'pefc_certificado',  # Normalización de nombre de columna
        'fsc_certificado': 'fsc_certificado',
        'fsc_100%': 'fsc_certificado',  # Normalización de nombre de columna
        '€/m³': '€/ud',  # Normalización de nombre de columna
        'cousa_certa': 'cousa_certa',
        'motivo_**': 'motivo_**',
        'nome': 'nome_empresa',
        'modo_alleamento': 'modo_de_alleamento',
        'oferta.1': 'oferta_€',
        'fianza_prov._*': 'fianza_prov',
        'oferta_€': 'oferta€',
        'oferta': 'oferta€',

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


        # Asegurarse de que las columnas sean únicas
        df.columns = pd.Series(df.columns).str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
        df.columns = [f"{col}_{i}" if list(df.columns).count(col) > 1 else col for i, col in enumerate(df.columns)]

        # Renombrar las columnas usando el diccionario de mapeo
        df.rename(columns=column_mapping, inplace=True)

        # Crear un DataFrame para almacenar los resultados
        result_df = pd.DataFrame(columns=df.columns)

        # Procesar cada fila para separar celdas unidas en columnas 0, 1, 2, 3 y 4
        for _, row in df.iterrows():
            # Identify rows with multiple values in columns 0, 1, 2, 3, 4
            merged_columns = [0, 1, 2, 3, 4]
            split_values = {}
            for col in merged_columns:
                if isinstance(row[df.columns[col]], str) and '\n' in row[df.columns[col]]:
                    split_values[col] = row[df.columns[col]].split('\n')
                else:
                    split_values[col] = [row[df.columns[col]]]

            max_splits = max(len(values) for values in split_values.values())

            for i in range(max_splits):
                new_row = row.copy()
                for col in merged_columns:
                    new_row[df.columns[col]] = split_values[col][i] if i < len(split_values[col]) else split_values[col][-1]
                result_df = pd.concat([result_df, pd.DataFrame([new_row])], ignore_index=True)

        # Rellenar los valores None con los valores de la fila superior
        result_df = result_df.ffill()

        # Filtrar filas que no siguen el patrón "Nº lote" o "XXX/YY"
        pattern = r"^(nº_lote|\d{3}/\d{2})$"
        result_df = result_df[result_df[result_df.columns[0]].astype(str).str.match(pattern)]

        # Extraer el año y la provincia de la URL y agregarlas como columnas
        province, year = extract_year_province(file_url)
        result_df['provincia'] = province
        result_df['año'] = year

        # Añadir la columna 'archivo' con el nombre del archivo
        result_df['archivo'] = file_name

        # Añadir los datos extraídos al DataFrame final
        final_df = pd.concat([final_df, result_df], ignore_index=True)

    # Eliminar columnas que no contienen ningún dato (None o NaN)
    final_df = final_df.dropna(axis=1, how='all')

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"subastas_galicia_cuadro_apertura_{timestamp}.csv"
    # Guardar el resultado en un archivo CSV
    final_df.to_csv(filename, index=False)

    return filename