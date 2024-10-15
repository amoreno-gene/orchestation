import requests
import zipfile
import os
import pyodbc
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

# Carpetas para descargas y procesado
download_folder = '/home/airflow/gcs/data/zips'  # Ruta en Composer
extracted_folder = '/home/airflow/gcs/data/extracted_accdb'
output_folder = '/home/airflow/gcs/data/destino_csvs'

# Asegúrate de que las carpetas de destino existen
for folder in [download_folder, extracted_folder, output_folder]:
    if not os.path.exists(folder):
        os.makedirs(folder)

# Función para descargar archivos ZIP
def download_zip_files():
    base_url = 'https://www.miteco.gob.es'
    page_url = f'{base_url}/es/biodiversidad/temas/inventarios-nacionales/inventario-forestal-nacional/cuarto_inventario.html'

    response = requests.get(page_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Extraer todos los enlaces que contengan ".zip" y cuyo nombre comience con "sig_"
    zip_links = soup.find_all('a', href=True)
    for link in zip_links:
        href = link['href']
        if href.endswith('.zip') and 'sig_' in href.split('/')[-1].lower():  # Busca enlaces que terminen en ".zip" y contengan "sig_"
            zip_url = base_url + href
            file_name = href.split('/')[-1]  # Nombre del archivo a partir del enlace
            zip_path = os.path.join(download_folder, file_name)

            # Descargar el archivo ZIP
            print(f"Descargando {zip_url} como {file_name}")
            response = requests.get(zip_url)
            with open(zip_path, 'wb') as f:
                f.write(response.content)

            # Extraer el contenido del ZIP
            extract_zip(zip_path)

# Función para extraer archivos ZIP
def extract_zip(zip_path):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extracted_folder)
    print(f"Extraído: {zip_path}")

# Función para procesar los archivos .accdb y generar un CSV por tabla
def process_accdb_files():
    generated_csvs = []  # Lista para almacenar los CSV generados

    # Iterar sobre los ficheros .accdb en la carpeta extraída
    for root, dirs, files in os.walk(extracted_folder):
        for file in files:
            if file.endswith('.accdb'):
                accdb_path = os.path.join(root, file)
                print(f"Procesando base de datos: {file}")

                # Conectar a la base de datos .accdb
                conn_str = (
                    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                    r'DBQ=' + accdb_path + ';'
                )
                conn = pyodbc.connect(conn_str)
                cursor = conn.cursor()

                # Obtener todas las tablas en la base de datos
                tables = cursor.tables(tableType='TABLE')
                for table in tables:
                    table_name = table.table_name
                    print(f"Procesando tabla: {table_name}")

                    # Leer los datos de la tabla en un DataFrame de pandas
                    query = f"SELECT * FROM [{table_name}]"
                    df = pd.read_sql(query, conn)

                    # Añadir columna 'source_file' para identificar el archivo accdb de origen
                    df['source_file'] = file

                    # Ruta del archivo CSV final común para esta tabla
                    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                    csv_file = os.path.join(output_folder, f"{table_name}_{timestamp}.csv")

                    # Guardar el archivo CSV
                    df.to_csv(csv_file, index=False, encoding='utf-8')
                    print(f"Guardado {csv_file}")

                    # Añadir el CSV generado a la lista
                    generated_csvs.append(csv_file)

                # Cerrar conexión con la base de datos actual
                cursor.close()
                conn.close()

    return generated_csvs  # Retornar lista de CSVs generados

# Función principal para ser llamada desde el orquestador
def extract_and_process_data():
    # Descargar y extraer los archivos ZIP
    download_zip_files()

    # Procesar los archivos accdb y generar CSV por tabla
    generated_csvs = process_accdb_files()

    # Retornar la lista de archivos CSV generados
    return generated_csvs
