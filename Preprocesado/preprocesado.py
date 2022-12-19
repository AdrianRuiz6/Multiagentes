"""Preprocesamiento"""
# pylint: disable=W1203
import os
import io
import re
import csv
import logging
import pandas as pd
from deep_translator import GoogleTranslator
from sqlalchemy import create_engine
import psycopg2

def main():
    '''Método main para el preprocesado de los datos'''
    # Creamos carpeta para descargar los archivos desde la BBDD
    if not os.path.exists('./files'):
        os.makedirs('./files')
    load_from_postgres()
    data_clean()
    upload_to_postgres()

# pylint: disable=C0103
def data_clean():
    '''
    Método para limpieza de datos conseguidos desde la BBDD Postgresql
    '''

    # Limpieza de dataset de tabla_ccaa

    dataframe_1 = pd.read_csv('tabla_ccaa.csv', sep = ';', decimal = ',')

    del dataframe_1['penetracion_pcto']
    del dataframe_1['valor_miles']
    del dataframe_1['gasto_capita']
    del dataframe_1['volumen_miles']
    del dataframe_1['columna_10']
    del dataframe_1['columna_11']

    dataframe_1 = dataframe_1.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    df1_final = dataframe_1.drop(dataframe_1[dataframe_1.ccaa != "Total Nacional"].index)
    del df1_final['ccaa']
    df1_final['producto'].replace({'TOTAL PATATAS': 'PATATAS'}, inplace=True)
    df1_final.drop(df1_final[df1_final.producto == "PATATAS FRESCAS"].index, inplace=True)
    df1_final.drop(df1_final[df1_final.producto == "PATATAS CONGELADAS"].index, inplace=True)
    df1_final.drop(df1_final[df1_final.producto == "PATATAS PROCESADAS"].index, inplace=True)
    df1_final.drop(df1_final[df1_final.producto == "T.HORTALIZAS FRESCAS"].index, inplace=True)
    df1_final.drop(df1_final[df1_final.producto == "OTR.HORTALIZAS/VERD."].index, inplace=True)
    df1_final.drop(df1_final[df1_final.producto == "VERD./HORT. IV GAMA"].index, inplace=True)
    df1_final.drop(df1_final[df1_final.producto == "T.FRUTAS FRESCAS"].index, inplace=True)
    df1_final.drop(df1_final[df1_final.producto == "OTRAS FRUTAS FRESCAS"].index, inplace=True)
    df1_final.drop(df1_final[df1_final.producto == "FRUTAS IV GAMA"].index, inplace=True)
    df1_final = df1_final.reset_index(drop = True)
    for index, value in df1_final['month'].iteritems():
        mapping = {
            'Enero': 1,
            'Febrero': 2,
            'Marzo': 3,
            'Abril': 4,
            'Mayo': 5,
            'Junio': 6,
            'Julio': 7,
            'Agosto': 8,
            'Septiembre': 9,
            'Octubre': 10,
            'Noviembre': 11,
            'Diciembre': 12
            }
        df1_final.loc[index, 'month'] = mapping[value]

    df1_final['fecha'] = df1_final['month'].astype(str) + '/' + df1_final['year'].astype(str)

    columns = df1_final.columns.tolist()
    columns.insert(0, columns.pop(columns.index('fecha')))
    df1_final = df1_final.reindex(columns=columns)

    del df1_final['year']
    del df1_final['month']

    df1_final['fecha'] = df1_final['fecha'].astype('string')
    df1_final['producto'] = df1_final['producto'].astype('string')

    df1_final['precio_medio_kg'] = df1_final['precio_medio_kg'].str.replace(',', '.').astype(float)
    df1_final['consumo_capita'] = df1_final['consumo_capita'].str.replace(',', '.').astype(float)

    df1_final.to_csv('./files/tabla_spain_limpio.csv', sep = ';', index = False)

    # Limpieza de dataset de tabla_excel

    # Declaración de variables
    excel = './files/tabla_excel.csv'
    products = pd.DataFrame(columns = ['consumo_hogares'])
    nueva_tabla_limpia = pd.DataFrame(columns = ['producto',
                    'precio_euro_kg', 'valor_miles', 'volumen_miles'])

    xl = pd.read_csv(excel, sep = ';', decimal = ',')

    # Separamos para obtener el listado de los productos que nos interesan
    products['consumo_hogares'] = xl.iloc[418:463]['consumo_hogares']

    result = pd.merge(products, xl, on = 'consumo_hogares')

    # Resolvemos problemas de tipos
    result = result.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    result = result.dropna()
    result['columna_1'] = result['columna_1'].replace('', '0,0')
    result['columna_2'] = result['columna_2'].replace('', '0,0')
    result['columna_4'] = result['columna_4'].replace('', '0,0')

    result['columna_1'] = result['columna_1'].str.replace(',', '.').astype('float')
    result['columna_2'] = result['columna_2'].str.replace(',', '.').astype('float')
    result['columna_4'] = result['columna_4'].str.replace(',', '.').astype('float')

    nueva_tabla_limpia[['producto', 'precio_euro_kg', 'valor_miles',
                    'volumen_miles']] = result[['consumo_hogares',
                                                'columna_4', 'columna_5', 'columna_6']]
    nueva_tabla_limpia = nueva_tabla_limpia.round(decimals=2)

    nueva_tabla_limpia.to_csv('./files/tabla_excel_limpio.csv', sep = ';', index = False)

    # Limpieza de dataset de tabla_import

    df = pd.read_csv('./files/tabla_import.csv', sep = ";", decimal = ',')
    df['flow'] = df['flow'].astype('category')
    df['reporter'] = df['reporter'].astype('category')
    df.value= df.value.replace({":":0})

    translator = GoogleTranslator(source="en", target="es")

    counts1 = df.groupby('indicators').size()
    df1 = df[(df.indicators == "VALUE_IN_EUROS")]
    df2 = df[(df.indicators == "QUANTITY_IN_100KG")]
    df2.index=df1.index
    df1.insert(7, "VALUE_IN_EUROS", df1.value, True)
    df1.insert(8, "QUANTITY_IN_100KG", df2.value, True)

    df1 = df1.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    df1.VALUE_IN_EUROS = df1.VALUE_IN_EUROS.replace({' ':''}, regex=True)
    df1.QUANTITY_IN_100KG = df1.QUANTITY_IN_100KG.replace({' ':''}, regex=True)
    df1['VALUE_IN_EUROS'] = df1['VALUE_IN_EUROS'].astype(int)
    df1['QUANTITY_IN_100KG'] = df1['QUANTITY_IN_100KG'].astype(int)

    del df1['indicators']
    del df1['value']

    df1.isnull().values.any()

    df1.set_index(['period'], inplace=True)

    df1 = df1.drop('Jan.-Dec. 2018')
    df1 = df1.drop('Jan.-Dec. 2019')
    df1 = df1.drop('Jan.-Dec. 2020')

    df1 = df1.reset_index()

    counts = df1.groupby('product').size()
    for i in counts.index:
        df1.product = df1.product.replace({i:translator.translate(i)})
    df1 = df1.drop(columns=['PARTNER'])

    df1 = df1.convert_dtypes()

    df1.rename(columns = {'period' : 'fecha',
                        'reporter' : 'pais',
                        'product' : 'producto',
                        'flow' : 'flow',
                        'VALUE_IN_EUROS' : 'valor_euros',
                        'QUANTITY_IN_100KG' : 'cantidad_100Kg'}, inplace = True)

    # Hay alguna columna que no se cambia al usar el metodo convert_dtypes
    # por lo que hay que cambiarla a mano
    df1['pais'] = df1['pais'].astype('string')

    # Aplica la función "convert_month_to_number" a la columna "fecha" usando la función .apply()
    df1["fecha"] = df1["fecha"].apply(convert_month_to_number)

    df1.to_csv('./files/tabla_import_limpio.csv', sep = ';', index = False)

    # Limpieza de dataset de tabla_covid

    data5 = pd.read_csv('./files/tabla_covid.csv', sep = ";", decimal = ',')

    data5 = data5[['countriesandterritories', 'month', 'year', 'cases', 'deaths',
            'continentexp', 'cumulative_cases_per_100000']]

    data5 = data5.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    data5.rename(columns = {'countriesandterritories' : 'pais',
                        'month' : 'mes',
                        'year' : 'año',
                        'cases' : 'casos',
                        'deaths' : 'muertes',
                        'continentexp' : 'continente',
                        'cumulative_cases_per_100000' : 'incicencia_acumulada'
                        }, inplace = True)

    data5['fecha'] = data5['mes'].astype(str) + '/' + data5['año'].astype(str)

    data5.set_index(['continente'], inplace=True)

    data5 = data5.drop('Africa')
    data5 = data5.drop('America')
    data5 = data5.drop('Asia')
    data5 = data5.drop('Oceania')
    data5 = data5.drop('Other')

    data5 = data5[['pais', 'fecha', 'casos', 'muertes', 'incidencia_acumulada']]

    data5['incidencia_acumulada']= data5['incidencia_acumulada'].str.replace(',', '.').astype(float)
    data5['incidencia_acumulada']= data5['incidencia_acumulada'].fillna(0.0)

    data5.set_index(['pais', 'fecha'], inplace=True)

    data5['casos_mes'] = data5.groupby(['pais', 'fecha'])['casos'].sum()
    data5['muertes_mes'] = data5.groupby(['pais', 'fecha'])['muertes'].sum()
    data5['incicencia_media_mes'] = data5.groupby(['pais', 'fecha'])['incidencia_acumulada'].mean()

    data5 = data5[['casos_mes', 'muertes_mes', 'incicencia_media_mes']]
    data5 = data5.drop_duplicates()

    data5 = data5.reset_index()

    data5['fecha'] = data5['fecha'].astype('string')
    data5['pais'] = data5['pais'].astype('string')

    columns_d5 = data5.columns.tolist()
    columns_d5.insert(0, columns_d5.pop(columns_d5.index('fecha')))
    data5 = data5.reindex(columns = columns_d5)

    data5.to_csv('./files/tabla_covid_limpio.csv', sep = ';', index = False)

def convert_month_to_number(param):
    """
    Utiliza una expresión regular para buscar el nombre del mes en el valor dado.
    """
    trad_meses = {
        "Jan": "1",
        "Feb": "2",
        "Mar": "3",
        "Apr": "4",
        "May": "5",
        "Jun": "6",
        "Jul": "7",
        "Aug": "8",
        "Sep": "9",
        "Oct": "10",
        "Nov": "11",
        "Dec": "12",
    }

    regex = r"^(\w+)"
    month_name = re.search(regex, param).group(1)

    # Utiliza el diccionario trad_meses para cambiar el nombre del mes por un número
    month_number = trad_meses[month_name]

    # Devuelve el valor dado con el mes en formato numérico
    return month_number + "/" + param[-4:]

# pylint: disable=W1514
# pylint: disable=C0103
def load_from_postgres():
    '''
    Método usado para descargar los ficheros obtenidos en la fase de extracción.
    '''
    # Conectamos a la base de datos
    engine = create_engine("postgresql+psycopg2://postgres:admin@localhost:5432/postgres")
    conn = engine.raw_connection()
    cur = conn.cursor()

    tables = ['tabla_ccaa', 'tabla_covid', 'tabla_excel', 'tabla_import']

    for table in tables:
        cur.execute(f'SELECT * FROM {table};')
        with open(f'./files/{table}.csv', 'w', newline='') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow([desc[0] for desc in cur.description])
            writer.writerows(cur)
        # Cambiar luego por un logging.debuf(...)
        logging.debug(f'FILE {table}.csv DOWNLOADED PROPERLY.')

    conn.close()

# pylint: disable=W1514
def upload_to_postgres():
    '''
    Método usado para subir los ficheros después de procesarlos de
    vuelta a la base de datos en unas tablas nuevas.
    '''
    engine = create_engine("postgresql+psycopg2://postgres:admin@localhost:5432/postgres")
    conn = engine.raw_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Crear tabla ds5
    tabla_covid_limpio = '''CREATE TABLE tabla_covid_limpio( \
        fecha char(12), pais char(50), casos_mes INTEGER,\
        muertes_mes INTEGER, incidencia_media_mes float);'''
    cur.execute(tabla_covid_limpio)

    # Crear tabla ds4
    tabla_import_limpio = '''CREATE TABLE tabla_import_limpio( \
        fecha char(12), pais char(200), producto char(500), flow char(6),\
        valor_euros INTEGER, cantidad_100kg INTEGER;'''
    cur.execute(tabla_import_limpio)

    # Crear tabla ds1
    tabla_spain_limpio = '''CREATE TABLE tabla_spain_limpio( \
        fecha char(12), producto char(50), precio_medio_kg float, consumo_capita float);'''
    cur.execute(tabla_spain_limpio)

    # Crear tabla fichero excel
    tabla_excel_limpio = '''CREATE TABLE tabla_excel_limpio( \
        producto char(30), precio_medio float, valor_miles float, volumen float);'''
    cur.execute(tabla_excel_limpio)

    aux_dict = {
        'tabla_spain_limpio.csv':'tabla_spain_limpio',
        'tabla_import_limpio.csv':'tabla_import_limpio',
        'tabla_covid_limpio.csv':'tabla_covid_limpio',
        'tabla_excel_limpio.csv':'tabla_excel_limpio'
    }

    # pylint: disable=C0206
    for file in aux_dict:
        with open(f'files/{file}', 'r') as fichero:
            # Nos daba error si le añadimos el parámetro 'header=True' al metodo copy_from.
            # Esto puede ser debido a que tengamos una version desactualizada de psycopg2 y
            # por falta de tiempo hemos recurrido a esto:
            # Al leer el fichero empezamos a leer desde la segunda linea para que no recoja
            # el header como una linea mas de informacion en los ficheros.

            resto_lineas = fichero.readlines()
            resto_lineas = resto_lineas[1:]

            # Lo convierto a un objeto StringIO para evitar errores con el método
            resto_lineas = io.StringIO(''.join(resto_lineas))
            cur.copy_from(resto_lineas, aux_dict[file], sep = ';')
        logging.debug(f'File {file} uploaded correctly at Postgresql')

    conn.commit()
    conn.close()

    print('Process finished')
