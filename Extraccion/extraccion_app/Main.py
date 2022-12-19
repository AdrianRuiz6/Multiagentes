# pylint: disable=C0103
'''
Clase que funciona como Main y para subir los archivos a la base de datos.
'''
import logging
import io
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import Extract

# pylint: disable=C0301
LINK_FOLDER_DRIVE = 'https://drive.google.com/drive/folders/1UNLGgdPmSqzmJXitIV6oriXyJUtLRA3s?usp=share_link'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# pylint: disable=W0702
def main():
    '''
    Llama a los metodos necesarios para descargar los archivos, cambiarles
    el formato a .csv y subirlos a la base de datos
    '''
    error_uploading = False
    error_formatting = False

    logging.info("Descargando carpeta de Google Drive ...")
    # Extrae datasets de una carpeta de Google Drive.
    error_drive = Extract.extract_folder_drive(LINK_FOLDER_DRIVE)

    logging.info("Descargando archivos usando web scrapping ...")
    # Extrae datasets de una web utilizando web scrapping.
    error_scrapping = Extract.extract_web_scrapping()

    # Si alguna de las dos llamadas anteriores ha dado error entonces mostrará si se ha descargado
    # correctamente o no la carpeta.
    if error_drive or error_scrapping:
        logging.critical("Carpeta 'Datasets_mineria' no descargada correctamnte.")
        return
    logging.info("Carpeta 'Datasets_mineria' correctamente descargada.")
    # Se llama a los dos metodos que cambiaran el formato de los archivos txt
    # y xlsx a un formato .csv más conveniente para interactuar con la base de datos.
    logging.info("Cambiando formato de archivos a .csv ...")
    try:
        xlsx_to_csv()
        txt_to_csv()
    except:
        error_formatting = True
    if error_formatting:
        logging.critical("Formato no cambiados correctamente.")
        return
    logging.info("Formato cambiado correctamente.")

    # Se llama al metodo que sube los archivos ya tranformados en .csv a la base de datos postgres.
    logging.info("Subiendo archivos a la base de datos ...")
    try:
        upload_to_postgres()
    except:
        error_uploading = True
    if error_uploading:
        logging.critical("Archivos no subidos correctamente.")
        return
    logging.info("Archivos subidos correctamente.")

def xlsx_to_csv():
    '''
    Lee el fichero excel original (mensual_CCAA_2018.xlsx), lo filtra para que solo
    tenga los datos de la comunidad de cataluña y elimina los datos innecesarios que
    contengan cadenas tipo string para que asi podamos convertir las columnas
    necesarias a tipo float.
    '''
    archivo = 'Datasets_mineria/mensual_CCAA_2018.xlsx'

    # Variables auxiliares
    lista_mes = []
    meses = {
        'Enero':'01',
        'Febrero':'02',
        'Marzo':'03',
        'Abril':'04',
        'Mayo':'05',
        'Junio':'06',
        'Julio':'07',
        'Agosto':'08',
        'Septiembre':'09',
        'Octubre':'10',
        'Noviembre':'11',
        'Diciembre':'12'
        }
    nombre_fichero = archivo[:-5]
    nombre_final = nombre_fichero+'.csv'

    # Bucle en el que accedemos mes a mes en el fichero
    for mes in meses:
        excel_file = pd.read_excel(archivo, sheet_name = mes)
        excel_file = excel_file[[
            'CONSUMO EN HOGARES',
            'Unnamed: 7',
            'Unnamed: 8',
            'Unnamed: 9',
            'Unnamed: 10',
            'Unnamed: 11',
            'Unnamed: 12'
            ]]
        # Almacenamos en una lista con la información de todos los meses
        lista_mes.append(excel_file)
        file_anual = pd.concat(lista_mes)

    # Seleccionamos valores para identificar las filas no deseadas
    not_wanted = ['CATALUÑA', 'CONSUMO X CAPITA']

    for value in not_wanted:
        file_anual = file_anual.drop(file_anual[file_anual['Unnamed: 7'] == value].index)

    file_anual[['Unnamed: 7', 'Unnamed: 8', 'Unnamed: 9',
                'Unnamed: 10', 'Unnamed: 11', 'Unnamed: 12']] = file_anual[[
                'Unnamed: 7','Unnamed: 8', 'Unnamed: 9',
                'Unnamed: 10','Unnamed: 11', 'Unnamed: 12']].astype(str)

    # file_anual = file_anual.convert_dtypes()
    file_anual['Unnamed: 7'] = file_anual['Unnamed: 7'].str.replace(',', '.').astype(float)
    file_anual['Unnamed: 8'] = file_anual['Unnamed: 8'].str.replace(',', '.').astype(float)
    file_anual['Unnamed: 9'] = file_anual['Unnamed: 9'].str.replace(',', '.').astype(float)
    file_anual['Unnamed: 10'] = file_anual['Unnamed: 10'].str.replace(',', '.').astype(float)
    file_anual['Unnamed: 11'] = file_anual['Unnamed: 11'].str.replace(',', '.').astype(float)
    file_anual['Unnamed: 12'] = file_anual['Unnamed: 12'].str.replace(',', '.').astype(float)

    # hacer que al acabar de mezclar los meses lo transforme a un fichero CSV
    file_anual.to_csv(nombre_final, sep = ';', index = False, decimal= ',')

def txt_to_csv():
    '''
    Convierte los ficheros txt a csv para que no de errores a la hora de subir a la BBDD.
    '''
    archivos = ['Datasets_mineria/Dataset5_Coronavirus_cases.txt',
        'Datasets_mineria/Dataset4-Importaciones_Espana.txt',
        'Datasets_mineria/Dataset1-Consumo_CCAA.txt']

    for file in archivos:
        nombre_df = file[0:-4] + '.csv'
        dataframe = pd.read_csv(file, sep="|")
        dataframe = dataframe.convert_dtypes()
        dataframe.to_csv(nombre_df, sep = ';', index = False, decimal= ',')

def upload_to_postgres():
    '''
    Crea las tablas en la base de datos necesarias para subir los archivos y posteriormente
    los sube uno a uno.
    '''
    engine = create_engine("postgresql+psycopg2://postgres:admin@localhost:5432/postgres")

    conn = engine.raw_connection()

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Crear tabla del dataset 5.
    tabla_covid = '''CREATE TABLE tabla_covid( \
        dateRep char(10), \
        day INTEGER, month INTEGER, year INTEGER, \
        cases INTEGER, deaths INTEGER, countriesAndTerritories char(50), \
        geoId char(10), countryterritoryCode char(10), popData2019 char(20), \
        continentExp char(16), cumulative_cases_per_100000 char(20) );'''
    cur.execute(tabla_covid)

    # Crear tabla del dataset 4.
    tabla_import = '''CREATE TABLE tabla_import( \
        period char(20), reporter char(200), partner char(2), product char(500), \
        flow char(6), indicators char(20), value char(10));'''
    cur.execute(tabla_import)

    # Crear tabla del dataset 1.
    tabla_ccaa = '''CREATE TABLE tabla_ccaa( \
        year char(4), month char(12), ccaa char(30), \
        producto char(50), volumen_miles char(20), valor_miles char(20), \
        precio_medio_kg char(20), penetracion_pcto char(20), consumo_capita char(20), \
        gasto_capita char(20),columna_10 char(10), columna_11 char(10) );'''
    cur.execute(tabla_ccaa)

    # Crea tabla del excel de Cataluña.
    tabla_excel = '''CREATE TABLE tabla_excel( \
        consumo_hogares char(50), columna_1 char(20), columna_2 char(20), \
        columna_3 char(20), columna_4 char(20), columna_5 char(20), columna_6 char(20));'''
    cur.execute(tabla_excel)

    aux_dict = {
        'Dataset1-Consumo_CCAA.csv':'tabla_ccaa',
        'Dataset4-Importaciones_Espana.csv':'tabla_import',
        'Dataset5_Coronavirus_cases.csv':'tabla_covid',
        'mensual_CCAA_2018.csv':'tabla_excel'
    }

    # pylint: disable=C0206
    for file in aux_dict:
        # pylint: disable=W1514
        with open(f'Datasets_mineria/{file}', 'r') as f:
            # Nos daba error si le añadimos el parámetro 'header=True' al metodo copy_from.
            # Esto puede ser debido a que tengamos una version desactualizada de psycopg2 y
            # por falta de tiempo hemos recurrido a esto:
            # Al leer el fichero empezamos a leer desde la segunda linea para que no recoja
            # el header como una linea mas de informacion en los ficheros.
            resto_lineas = f.readlines()
            resto_lineas = resto_lineas[1:]

            # Lo convierto a un objeto StringIO para evitar errores con el método
            resto_lineas = io.StringIO(''.join(resto_lineas))
            cur.copy_from(resto_lineas, aux_dict[file], sep = ';')
            # pylint: disable=W1203
            logging.debug(f"Archivo '{file}' subido correctamente.")
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
