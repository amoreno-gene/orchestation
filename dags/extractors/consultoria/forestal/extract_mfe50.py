import os
import requests
import zipfile
import geopandas as gpd
import pandas as pd
from bs4 import BeautifulSoup
import logging
from datetime import datetime

# Configuración de la URL base y del directorio de trabajo
BASE_URL = "https://www.miteco.gob.es/es/biodiversidad/servicios/banco-datos-naturaleza/informacion-disponible/mfe50_descargas_ccaa.html"
DOWNLOAD_DIR = "/home/airflow/gcs/data/descargas_mfe50"  # Ruta en Composer
EXTRACT_DIR = "/home/airflow/gcs/data/shapefiles_mfe50"
CSV_DIR = "/home/airflow/gcs/data/csv_mfe50"
FINAL_CSV = "/home/airflow/gcs/data/csv_mfe50/final_mfe50.csv"

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Crear los directorios si no existen
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(EXTRACT_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)


# Hardcodear los links de descarga
page_links = [
    "/es/biodiversidad/servicios/banco-datos-naturaleza/informacion-disponible/mfe50_descargas_aragon.html",
    "/es/biodiversidad/servicios/banco-datos-naturaleza/informacion-disponible/mfe50_descargas_andalucia.html",
    "/es/biodiversidad/servicios/banco-datos-naturaleza/informacion-disponible/mfe50_descargas_catalunya.html",
    "/es/biodiversidad/servicios/banco-datos-naturaleza/informacion-disponible/mfe50_descargas_castilla_la_mancha.html",
    "/es/biodiversidad/servicios/banco-datos-naturaleza/informacion-disponible/mfe50_descargas_castilla_y_leon.html",
    "/es/biodiversidad/servicios/banco-datos-naturaleza/informacion-disponible/mfe50_descargas_comunidad_valenciana.html",
    "/es/biodiversidad/servicios/banco-datos-naturaleza/informacion-disponible/mfe50_descargas_extremadura.html",
    "/es/biodiversidad/servicios/banco-datos-naturaleza/informacion-disponible/mfe50_descargas_galicia.html",
    "/es/biodiversidad/servicios/banco-datos-naturaleza/informacion-disponible/mfe50_descargas_comunidad_madrid.html",
    "/es/biodiversidad/servicios/banco-datos-naturaleza/informacion-disponible/mfe50_descargas_murcia.html",
    "/es/biodiversidad/servicios/banco-datos-naturaleza/informacion-disponible/mfe50_descargas_navarra.html",
    "/es/biodiversidad/servicios/banco-datos-naturaleza/informacion-disponible/mfe50_descargas_pais_vasco.html",
    "/es/biodiversidad/servicios/banco-datos-naturaleza/informacion-disponible/mfe50_descargas_rioja.html",
    "/es/biodiversidad/servicios/banco-datos-naturaleza/informacion-disponible/mfe50_descargas_canarias.html",
    "/es/biodiversidad/servicios/banco-datos-naturaleza/informacion-disponible/mfe50_descargas_baleares.html",
    "/es/biodiversidad/servicios/banco-datos-naturaleza/informacion-disponible/mfe50_descargas_asturias.html",
    "/es/biodiversidad/servicios/banco-datos-naturaleza/informacion-disponible/mfe50_descargas_cantabria.html"
]

# Función para descargar y extraer archivos ZIP
def download_and_extract_zip():
    for page_link in page_links:
        page_url = f"https://www.miteco.gob.es{page_link}"
        logger.info(f"Accediendo a la página: {page_url}")
        page_response = requests.get(page_url)
        page_soup = BeautifulSoup(page_response.content, "html.parser")

        # Encontrar todas las URLs de descarga de archivos ZIP en la página
        zip_links = page_soup.find_all("a", href=True)
        zip_links = [link['href'] for link in zip_links if link['href'].endswith('.zip')]

        for zip_link in zip_links:
            zip_url = zip_link if zip_link.startswith("http") else f"https://www.miteco.gob.es{zip_link}"
            zip_name = zip_url.split("/")[-1]
            zip_path = os.path.join(DOWNLOAD_DIR, zip_name)

            # Descargar el archivo ZIP si no existe
            if not os.path.exists(zip_path):
                logger.info(f"Descargando {zip_name} desde {zip_url}...")
                with requests.get(zip_url, stream=True) as r:
                    if r.status_code == 200:
                        with open(zip_path, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                f.write(chunk)
                        logger.info(f"Descarga de {zip_name} completada.")
                    else:
                        logger.error(f"Error al descargar {zip_name}: Código de estado {r.status_code}")
            else:
                logger.info(f"El archivo {zip_name} ya existe. Saltando descarga.")

            # Extraer el archivo ZIP
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(EXTRACT_DIR)
                logger.info(f"Archivo {zip_name} extraído correctamente.")
            except zipfile.BadZipFile:
                logger.error(f"Error al extraer {zip_name}: archivo ZIP corrupto.")

# Función para procesar shapefiles y combinar en un solo CSV
def process_shapefiles_to_single_csv():
    combined_df = pd.DataFrame()

    # Buscar y convertir los shapefiles a CSV
    for root, _, files in os.walk(EXTRACT_DIR):
        for file in files:
            if file.endswith(".shp"):
                shp_path = os.path.join(root, file)

                # Leer el shapefile con GeoPandas y agregar al DataFrame combinado
                logger.info(f"Convirtiendo {file} a DataFrame...")
                try:
                    gdf = gpd.read_file(shp_path)
                    df = pd.DataFrame(gdf)
                    # Añadir columna de provincia basada en el nombre del archivo shapefile
                    provincia = os.path.basename(root)
                    df["provincia"] = provincia
                    combined_df = pd.concat([combined_df, df], ignore_index=True)
                    logger.info(f"Conversión de {file} completada.")
                except Exception as e:
                    logger.error(f"Error al convertir {file} a DataFrame: {e}")

    # Guardar el DataFrame combinado en un archivo CSV
    combined_df.to_csv(FINAL_CSV, index=False)
    logger.info(f"CSV final guardado en {FINAL_CSV}")

    # Retornar la lista con el único CSV generado
    return [FINAL_CSV]

# Función principal para ser llamada desde el orquestador
def extract_and_process_data():
    # Descargar y extraer los archivos ZIP
    download_and_extract_zip()

    # Procesar los shapefiles y generar un CSV final
    generated_csvs = process_shapefiles_to_single_csv()

    # Retornar la lista de archivos CSV generados
    return generated_csvs