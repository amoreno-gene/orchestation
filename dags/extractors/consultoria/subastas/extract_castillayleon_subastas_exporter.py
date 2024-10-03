import requests
import pdfplumber
import pandas as pd
import os
from datetime import datetime
from urllib.parse import urlparse, unquote


def extract_and_process_data():

    # Function to download a file from a URL
    def download_pdf(url, output_path):
        response = requests.get(url)
        with open(output_path, 'wb') as f:
            f.write(response.content)

    # Function to get a valid filename from a URL
    def get_valid_filename(url):
        path = urlparse(url).path
        filename = os.path.basename(path)
        # Decode the filename
        filename = unquote(filename)
        # Remove invalid characters from the filename
        filename = filename.split('?')[0]
        return filename

    # Function to transform the DataFrame and add the 'Etapa' column
    def transform_dataframe(df):
        df_filtered = df[df.iloc[:, 0].notna()].copy()
        df_filtered['Etapa'] = df_filtered.iloc[:, 1].where(df_filtered.iloc[:, 1].str.contains('PRIMAVERA|OTOÑO|Extra', case=False, na=False)).ffill()
        df_filtered = df_filtered[~df_filtered.iloc[:, 1].str.contains('PRIMAVERA|OTOÑO|Extra', case=False, na=False)]
        df_filtered = df_filtered[~df_filtered.iloc[:, 1].str.contains('Total general|Total', case=False, na=False)]
        df_filtered = df_filtered[df_filtered[0] != '']
        df_filtered = df_filtered[~(df_filtered == '').all(axis=1)]
        return df_filtered

    # Function to clean the DataFrame
    def clean_dataframe(df):
        # Drop columns with all NaN, None, or empty values
        df = df.dropna(axis=1, how='all')
        df = df.loc[:, ~(df.isna().all() | (df == '').all())]

        # Drop rows that are completely empty
        df = df.dropna(axis=0, how='all')
        return df

    # Function to move a column to the front
    def move_column_to_front(df, column_name):
        col = df.pop(column_name)
        df.insert(0, column_name, col)
        return df

    # Function to rename DataFrame columns
    def rename_columns(df):
        df = df.dropna(subset=[df.columns[0]])  # Drop rows with NaN in the first column
        column_names = [
            'Etapa', 'Provincia', 'Nº Lote ofertado', 'Nº Lote certificado ofertado', 'Superficie ha ofertado', 
            'Volumen m3 ofertado', 'Precio ofertado', 'Precio medio ponderado ofertado', 'Nº Lote adjudicado', 
            'Nº Lote certificado adjudicado', 'Superficie ha adjudicado', 'Volumen m3 adjudicado', 'Precio adjudicado', 
            'Precio medio ponderado adjudicado', 'Volumen desierto', 'prc de volumen desierto', 'Aumento de precio'
        ]
        # Adjust the number of columns if there are more columns than names available
        df.columns = column_names[:len(df.columns)]
        return df

    # Function to add the year column
    def add_year_column(df, title):
        year = ''.join(filter(str.isdigit, title))
        df['Año'] = year
        return df

    # Function to remove columns with all 'None' values
    def remove_none_columns(df):
        df = df.dropna(axis=1, how='all')
        return df

    # Function to extract and filter tables from a page
    def extract_and_filter_tables(file_path, page_nums):
        all_tables = []
        with pdfplumber.open(file_path) as pdf:
            for page_num in page_nums:
                page = pdf.pages[page_num]
                tables = page.extract_tables()
                text = page.extract_text()
                lines = text.split('\n')
                titles = [line for line in lines if 'MADERA DE' in line or 'LEÑA DE' in line]
                for i, table in enumerate(tables):
                    if i < len(titles):
                        title = titles[i]
                        df = pd.DataFrame(table)
                        df_transformed = transform_dataframe(df)
                        df_cleaned = clean_dataframe(df_transformed)
                        df_cleaned = move_column_to_front(df_cleaned, 'Etapa')
                        df_renamed = rename_columns(df_cleaned)
                        df_final = add_year_column(df_renamed, title)
                        df_final = remove_none_columns(df_final)
                        df_final['Tipo de Madera'] = title  # Add the title as the type of wood
                        all_tables.append(df_final)
                        print(f'Tabla {i} procesada en la página {page_num + 1}.')
        return all_tables

    # Dictionary of download links for the PDFs and the pages to extract
    pdf_info = {
        'https://medioambiente.jcyl.es/web/jcyl/binarios/613/679/Analisis%20resultados%20subasta%20madera%202021_CMVyOT.pdf?blobheader=application%2Fpdf%3Bcharset%3DUTF-8&blobnocache=true': [32, 33],
        'https://medioambiente.jcyl.es/web/jcyl/binarios/734/325/Analisis%20resultados%20subasta%20madera%202020_CFYMA.pdf?blobheader=application%2Fpdf%3Bcharset%3DUTF-8&blobnocache=true': [26]
        
    }

    # Folder to save the downloaded PDFs
    download_folder = './pdf_files/'
    os.makedirs(download_folder, exist_ok=True)

    # List to store all tables from all PDFs
    all_tables = []

    # Download and process each PDF
    for url, page_nums in pdf_info.items():
        filename = get_valid_filename(url)
        file_path = os.path.join(download_folder, filename)
        download_pdf(url, file_path)
        tables = extract_and_filter_tables(file_path, page_nums)
        all_tables.extend(tables)

    # Concatenate all tables into a single DataFrame
    final_df = pd.concat(all_tables, ignore_index=True)



    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"subastas_castillayleon_anual{timestamp}.csv"
    # Guardar el resultado en un archivo CSV
    final_df.to_csv(filename, index=False)

    return filename