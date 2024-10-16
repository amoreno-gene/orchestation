import os
import requests
import zipfile
import dask_geopandas as dgpd
import pandas as pd
from bs4 import BeautifulSoup
import logging
from concurrent.futures import ThreadPoolExecutor

# Configuración de la URL base y del directorio de trabajo
BASE_URL = "https://www.miteco.gob.es/es/biodiversidad/servicios/banco-datos-naturaleza/informacion-disponible/mfe50_descargas_ccaa.html"
DOWNLOAD_DIR = "/home/airflow/gcs/data/descargas_mfe50"  # Ruta en Composer
EXTRACT_DIR = "/home/airflow/gcs/data/shapefiles_mfe50"
CSV_DIR = "/home/airflow/gcs/data/csv_mfe50"
FINAL_CSV_PATH = "/home/airflow/gcs/data/csv_mfe50/final_combined.csv"

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

def download_and_extract(zip_links):
    def download_and_extract_zip(zip_url):
        zip_name = zip_url.split("/")[-1]
        zip_path = os.path.join(DOWNLOAD_DIR, zip_name)

        # Descargar el archivo ZIP si no existe
        if not os.path.exists(zip_path):
            logger.info(f"Descargando {zip_name} desde {zip_url}...")
            try:
                with requests.get(zip_url, stream=True) as r:
                    if r.status_code == 200:
                        with open(zip_path, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                f.write(chunk)
                        logger.info(f"Descarga de {zip_name} completada.")
                    else:
                        logger.error(f"Error al descargar {zip_name}: Código de estado {r.status_code}")
            except Exception as e:
                logger.error(f"Error al descargar {zip_name}: {e}")
        else:
            logger.info(f"El archivo {zip_name} ya existe. Saltando descarga.")

        # Extraer el archivo ZIP
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(EXTRACT_DIR)
            logger.info(f"Archivo {zip_name} extraído correctamente.")
        except zipfile.BadZipFile:
            logger.error(f"Error al extraer {zip_name}: archivo ZIP corrupto.")

    # Descargar y extraer archivos en paralelo
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(download_and_extract_zip, zip_links)

def extract_and_process_data():
    csv_files = []  # Lista para almacenar los CSV generados

    # Descargar y extraer archivos ZIP
    for page_link in page_links:
        page_url = f"https://www.miteco.gob.es{page_link}"
        logger.info(f"Accediendo a la página: {page_url}")
        page_response = requests.get(page_url)
        page_soup = BeautifulSoup(page_response.content, "html.parser")

        # Encontrar todas las URLs de descarga de archivos ZIP en la página
        zip_links = page_soup.find_all("a", href=True)
        zip_links = [link['href'] for link in zip_links if link['href'].endswith('.zip')]
        zip_links = [link if link.startswith("http") else f"https://www.miteco.gob.es{link}" for link in zip_links]

        # Descargar y extraer los archivos ZIP en paralelo
        download_and_extract(zip_links)

    # Convertir shapefiles a CSV usando Dask GeoPandas
    for root, _, files in os.walk(EXTRACT_DIR):
        for file in files:
            if file.endswith(".shp"):
                shp_path = os.path.join(root, file)
                csv_name = f"{os.path.splitext(file)[0]}.csv"
                csv_path = os.path.join(CSV_DIR, csv_name)

                # Leer el shapefile con Dask GeoPandas y guardarlo como CSV
                logger.info(f"Convirtiendo {file} a CSV usando Dask...")
                try:
                    gdf = dgpd.read_file(shp_path,chunksize=10000)  # Leer con Dask GeoPandas
                    df = gdf.compute()  # Convierte a un DataFrame de Pandas
                    df.to_csv(csv_path, index=False)
                    csv_files.append(csv_path)
                    logger.info(f"Conversión de {file} a CSV completada.")
                except Exception as e:
                    logger.error(f"Error al convertir {file} a CSV: {e}")

    # Combinar todos los CSVs en uno solo
    logger.info("Combinando todos los CSVs en uno solo...")
    try:
        combined_df = pd.concat((pd.read_csv(csv_file, chunksize=10000) for csv_file in csv_files), ignore_index=True)
        combined_df.to_csv(FINAL_CSV_PATH, index=False)
        logger.info(f"CSV combinado guardado en {FINAL_CSV_PATH}")
    except Exception as e:
        logger.error(f"Error al combinar los CSVs: {e}")

    # Retornar la lista de archivos CSV generados (en este caso, solo el archivo final)
    return [FINAL_CSV_PATH]