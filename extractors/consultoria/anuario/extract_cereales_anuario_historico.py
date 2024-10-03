import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import re
import unidecode
from datetime import datetime


def extract_and_process_data():
    # Base URLs for the Anuario de Estadística
    base_urls = {
        "2003-2007": "https://www.mapa.gob.es/es/estadistica/temas/publicaciones/anuario-de-estadistica/{year}/default.aspx",
        "2008-2017": "https://www.mapa.gob.es/es/estadistica/temas/publicaciones/anuario-de-estadistica/{year}/default.aspx?parte=3&capitulo=13",
        "2018-2022": "https://www.mapa.gob.es/es/estadistica/temas/publicaciones/anuario-de-estadistica/{year}/default.aspx?parte=3&capitulo=07",
    }

    # Years to process
    years = list(range(2018, 2023))

    # Directory to save downloaded files
    download_dir = 'anuario_estadistica_files'
    os.makedirs(download_dir, exist_ok=True)

    # Function to determine the URL for a given year
    def get_base_url(year):
        if 2003 <= year <= 2007:
            return base_urls["2003-2007"]
        elif 2008 <= year <= 2017:
            return base_urls["2008-2017"]
        elif 2018 <= year <= 2022:
            return base_urls["2018-2022"]
        return None

    # Function to download files
    def download_file(url, year, filename):
        response = requests.get(url)
        if response.status_code == 200:
            filepath = os.path.join(download_dir, f'{year}_{filename}')
            with open(filepath, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded: {filepath}")
            return filepath
        else:
            print(f"Failed to download file from {url}")
        return None

    # Function to scrape and download Excel files
    def scrape_and_download(year):
        base_url = get_base_url(year).format(year=year)
        response = requests.get(base_url)
        if response.status_code != 200:
            print(f"Failed to access page for year {year}")
            return []

        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', href=True)
        excel_links = [link['href'] for link in links if link['href'].lower().endswith(('.xls', '.xlsx'))]

        downloaded_files = []
        for link in excel_links:
            file_url = link if link.startswith('http') else 'https://www.mapa.gob.es' + link
            filename = file_url.split('/')[-1]
            downloaded_file = download_file(file_url, year, filename)
            if downloaded_file:
                downloaded_files.append(downloaded_file)
        return downloaded_files

    # Process each downloaded Excel file and save matching sheets as CSV
    def process_and_save_csv(file_path):
        excel_file = pd.ExcelFile(file_path)
        pattern = re.compile(r'-*Análisis provincial de superficie, rendimiento y producción.*')

        for sheet_name in excel_file.sheet_names:
            df = excel_file.parse(sheet_name, header=None)
            if len(df) > 2:
                row_3_text = ' '.join(df.iloc[2].astype(str).values)
                if pattern.search(row_3_text):
                    csv_filename = f'{os.path.splitext(file_path)[0]}_{sheet_name}.csv'
                    df.to_csv(csv_filename, index=False, header=False)
                    print(f"Saved CSV: {csv_filename}")

    # Normalize and process text
    def normalize_text(text):
        return unidecode.unidecode(text.lower()).replace('–', '-').replace(' ', '')

    # Process matching sheet
    def process_matching_sheet(df, harvest_type, species, year, combined_df):
        num_columns = df.shape[1]
        if num_columns == 7:
            df.columns = ['Province', 'Superficie_Secano', 'Superficie_Regadio', 'Superficie_Total',
                          'Rendimiento_Secano', 'Rendimiento_Regadio', 'Paja_Cosechada']
            df['Grano_Produccion'] = None
        elif num_columns == 8:
            df.columns = ['Province', 'Superficie_Secano', 'Superficie_Regadio', 'Superficie_Total',
                          'Rendimiento_Secano', 'Rendimiento_Regadio', 'Grano_Produccion', 'Paja_Cosechada']
        elif num_columns == 9:
            df.columns = ['Province', 'Superficie_Secano', 'Superficie_Regadio', 'Superficie_Protegido', 'Superficie_Total',
                          'Rendimiento_Secano', 'Rendimiento_Regadio', 'Rendimiento_Protegido', 'Grano_Produccion', 'Paja_Cosechada']
        elif num_columns == 10:
            df.columns = ['Province', 'Superficie_Secano', 'Superficie_Regadio', 'Superficie_Total', 'Rendimiento_Fibra_Secano',
                          'Rendimiento_Fibra_Regadio', 'Rendimiento_Semilla_Secano', 'Rendimiento_Semilla_Regadio',
                          'Fibra_Produccion', 'Semilla_Produccion']
        else:
            df.columns = ['Province', 'Superficie_Secano', 'Superficie_Regadio', 'Superficie_Total',
                          'Rendimiento_Secano', 'Rendimiento_Regadio', 'Grano_Produccion', 'Paja_Cosechada']

        df = df.drop(range(7))
        df = df.dropna(subset=['Province'])
        df['Province_normalized'] = df['Province'].apply(normalize_text)
        df['Harvest_Type'] = harvest_type
        df['Species'] = species
        df['Year'] = year
        combined_df = pd.concat([combined_df, df], ignore_index=True)
        return combined_df

    # Combine all matching CSV files into one DataFrame
    def combine_csv_files(input_dir):
        combined_df = pd.DataFrame()
        for root, _, files in os.walk(input_dir):
            for file in files:
                if file.lower().endswith('.csv'):
                    file_path = os.path.join(root, file)
                    combined_df = process_excel(file_path, combined_df)
        return combined_df

    # Process Excel file to match regex and extract relevant data
    def process_excel(file_path, combined_df):
        df = pd.read_csv(file_path, header=None)
        pattern = re.compile(r'-*Análisis provincial de superficie, rendimiento y producción.*')
        species_pattern = re.compile(r'\d+\.\d+\.\d+\.\d+\.\s+(\w+)\s+(\w+)-(\w+):\s*Análisis provincial de superficie, rendimiento y producción,\s*(\d+)')

        if len(df) > 2:
            row_3_text = ' '.join(df.iloc[2].astype(str).values)
            if pattern.search(row_3_text):
                match = species_pattern.search(row_3_text)
                if match:
                    species, sub_species_type, sub_species, year = match.groups()
                    if species.lower() in ["cereales", "leguminosas", "hortalizas", "cultivos industriales"]:
                        combined_df = process_matching_sheet(df, f"{species} {sub_species_type}", sub_species, year, combined_df)
        return combined_df

    # Main workflow
    all_downloaded_files = []
    for year in years:
        downloaded_files = scrape_and_download(year)
        all_downloaded_files.extend(downloaded_files)

    for file_path in all_downloaded_files:
        process_and_save_csv(file_path)

    combined_df = combine_csv_files('anuario_estadistica_files')

            # Guardar el DataFrame combinado en un archivo CSV
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"final_output_cereales{timestamp}.csv"
    combined_df.to_csv(filename, index=False)
    return filename