"""Preprocesamiento"""
import os
import re
import pandas as pd
from pandas_profiling import ProfileReport
from deep_translator import GoogleTranslator

ruta=os.getcwd()

dataframe_1 = pd.read_csv(ruta + "\\Datasets\\Dataset1.- "+
"DatosConsumoAlimentarioMAPAporCCAA.txt", sep="|")
del dataframe_1["Penetración (%)"]
del dataframe_1["Valor (miles de €)"]
del dataframe_1["Gasto per capita"]
del dataframe_1["Volumen (miles de kg)"]
del dataframe_1["Unnamed: 10"]
del dataframe_1["Unnamed: 11"]
dataframe_1.rename({'Precio medio kg': 'precio_medio_kg'}, axis=1, inplace=True)
dataframe_1.rename({'Consumo per capita': 'consumo_per_capita'}, axis=1, inplace=True)
dataframe_1.rename({'Producto': 'producto'}, axis=1, inplace=True)

dataframe_1.isnull().values.all()

df1_final = dataframe_1.drop(dataframe_1[dataframe_1.CCAA != "Total Nacional"].index)
del df1_final["CCAA"]
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
for index, value in df1_final['Mes'].iteritems():
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
    df1_final.loc[index, 'Mes'] = mapping[value]

df1_final['fecha'] = df1_final['Mes'].astype(str) + '/' + df1_final['Año'].astype(str)

columns = df1_final.columns.tolist()
columns.insert(0, columns.pop(columns.index('fecha')))
df1_final = df1_final.reindex(columns=columns)

del df1_final["Año"]
del df1_final["Mes"]

df1_final['fecha'] = df1_final['fecha'].astype('string')
df1_final['producto'] = df1_final['producto'].astype('string')

df1_final['precio_medio_kg'] = df1_final['precio_medio_kg'].str.replace(',', '.').astype(float)
df1_final['consumo_per_capita'] =df1_final['consumo_per_capita'].str.replace(','
, '.').astype(float)

profile = ProfileReport(df1_final, title="Consumo alimentario en España",
html={'style':{'full_width': True}})
profile.to_notebook_iframe()

#---------------------------------------------------------------------------------------------------

# Declaración de variables
archivos = [ruta+'\\Datasets\\2018datosmensualesdelpaneldeconsumoalimentarioenhogares_tcm30-'+
        '520451_tcm30-520451.xlsx',
        ruta+'\\Datasets\\2019datosmensualesdelpaneldeconsumoalimentarioenhogares_tcm30'+
        '-5204501_tcm30-520450.xlsx',
        ruta+'\\Datasets\\2020-datos-mensuales-panel-hogares-ccaa-rev-nov2021_tcm30-540244.xlsx']

POBLACION = ruta+'\\Datasets\\2915bsc.csv'

# Indice para avanzar en el fichero de forma inversa
i = 2

lista_mes = []
lista_mes_2020 = []
meses = {'Enero':'01', 'Febrero':'02', 'Marzo':'03', 'Abril':'04', 'Mayo':'05',
        'Junio':'06', 'Julio':'07', 'Agosto':'08', 'Septiembre':'09', 'Octubre':'10',
        'Noviembre':'11', 'Diciembre':'12'}
FIRST = True
pob = pd.read_csv(POBLACION, sep=';', encoding='latin-1')
products = pd.DataFrame(columns = ['CONSUMO EN HOGARES'])
mes_aux = pd.DataFrame(columns = ['Producto', 'Precio AND (€/Kg)', 'Precio MAD (€/Kg)',
            'Precio CLM (€/Kg)', 'Precio CYL (€/Kg)', 'Precio CAT (€/Kg)', 'Valor AND (miles €)',
            'Valor MAD (miles €)', 'Valor CLM (miles €)', 'Valor CYL (miles €)',
            'Valor CAT (miles €)', 'Volumen AND (miles kg)', 'Volumen MAD (miles kg)',
            'Volumen CLM (miles kg)', 'Volumen CYL (miles kg)', 'Volumen CAT (miles kg)',
            'Poblacion AND', 'Poblacion MAD', 'Poblacion CLM', 'Poblacion CYL', 'Poblacion CAT'])

# Bucle para la limpieza y obtención de los datos de los datasets originales
for file in archivos:
    ANO=file[len(ruta)+10:len(ruta)+14]
    CONDITION = True
  # Diferenciamos entre 2018/19 y 2020 al cambiar de formato para nombrar las hojas del excel
    if ANO == '2020':
        meses = {k.lower(): v for k, v in meses.items()}
        CONDITION = False

    for mes in meses:
        xl = pd.read_excel(file, sheet_name = mes)
        mes_2020 = mes_aux = pd.DataFrame(columns = ['Producto', 'Fecha', 'Precio AND (€/Kg)',
                                                 'Precio MAD (€/Kg)', 'Valor AND (miles €)',
                                                 'Valor MAD (miles €)', 'Volumen AND (miles kg)',
                                                 'Volumen MAD (miles kg)'])
    # Cogemos los productos que nos interesan
        if mes == 'Enero' and FIRST:
            products['CONSUMO EN HOGARES'] = xl.iloc[420:465]['CONSUMO EN HOGARES']
            FIRST = False

    # Juntamos el dataset base con la lista de productos con un merge
    # para obtener solo los datos de interés
        first_month = pd.merge(products, xl, on='CONSUMO EN HOGARES', how='left')
        mes_aux[['Producto', 'Precio AND (€/Kg)', 'Precio MAD (€/Kg)', 'Precio CLM (€/Kg)',
            'Precio CYL (€/Kg)', 'Precio CAT (€/Kg)', 'Valor AND (miles €)', 'Valor MAD (miles €)',
            'Valor CLM (miles €)','Valor CYL (miles €)', 'Valor CAT (miles €)',
            'Volumen AND (miles kg)','Volumen MAD (miles kg)', 'Volumen CLM (miles kg)',
            'Volumen CYL (miles kg)', 'Volumen CAT (miles kg)']] = first_month[['CONSUMO'+
            ' EN HOGARES','Unnamed: 40', 'Unnamed: 46', 'Unnamed: 52', 'Unnamed: 64', 'Unnamed: 10',
            'Unnamed: 41', 'Unnamed: 47', 'Unnamed: 53', 'Unnamed: 65', 'Unnamed: 11',
            'Unnamed: 42', 'Unnamed: 48', 'Unnamed: 54', 'Unnamed: 66', 'Unnamed: 12',]]
        mes_aux['Fecha'] = meses[mes]+'/'+ANO

    # Filtramos los datos de la población de cada comunidad y los almacenamos
        mes_aux['Poblacion AND'] = pob.iloc[3+i]['Total']
        mes_aux['Poblacion MAD'] = pob.iloc[39+i]['Total']
        mes_aux['Poblacion CLM'] = pob.iloc[24+i]['Total']
        mes_aux['Poblacion CYL'] = pob.iloc[21+i]['Total']
        mes_aux['Poblacion CAT'] = pob.iloc[27+i]['Total']

    # Limpieza de duplicados
        mes_aux = mes_aux.drop_duplicates(subset=['Producto'], keep='first')

    # Unimos los datos de cada mes y los almacenamos en un dataset aparte
        lista_mes.append(mes_aux)
        dataset_comunidades = pd.concat(lista_mes)
    i-=1

# Limpieza de nulos y normalización de los datos
dataset_comunidades = dataset_comunidades.convert_dtypes()
dataset_comunidades = dataset_comunidades.fillna(0.0)

# Nos quitamos los valores que no nos interesan
not_wanted = ['T.FRUTAS FRESCAS', 'T.HORTALIZAS FRESCAS', 'OTRAS FRUTAS FRESCAS',
              'OTR.HORTALIZAS/VERD.', 'VERD./HORT. IV GAMA']
ds_clean = dataset_comunidades

for product in not_wanted:
    ds_clean = ds_clean.drop(ds_clean[ds_clean['Producto'] == product].index)

ds_clean = ds_clean.round(decimals=3)
ds_clean.reset_index(drop=True)

profile = ProfileReport(ds_clean, title="Andalucia-Madrid", html={'style':{'full_width': True}})
profile.to_notebook_iframe()

#-----------------------------------------------------------------------------------------

df = pd.read_csv(ruta + "\\Datasets\\Dataset4.- Comercio Exterior de España.txt", sep ="|")
df['FLOW'] = df['FLOW'].astype('category')
df['REPORTER'] = df['REPORTER'].astype('category')
df.Value= df.Value.replace({":":0})

translator = GoogleTranslator(source="en", target="es")

counts1 = df.groupby("INDICATORS").size()
df1 = df[(df.INDICATORS == "VALUE_IN_EUROS")]
df2 = df[(df.INDICATORS == "QUANTITY_IN_100KG")]
df2.index=df1.index
df1.insert(7, "VALUE_IN_EUROS", df1.Value, True)
df1.insert(8, "QUANTITY_IN_100KG", df2.Value, True)

df1.VALUE_IN_EUROS= df1.VALUE_IN_EUROS.replace({' ':''}, regex=True)
df1.QUANTITY_IN_100KG= df1.QUANTITY_IN_100KG.replace({' ':''}, regex=True)
df1['VALUE_IN_EUROS'] = df1['VALUE_IN_EUROS'].astype(int)
df1['QUANTITY_IN_100KG'] = df1['QUANTITY_IN_100KG'].astype(int)


del df1['INDICATORS']
del df1['Value']

df1.isnull().values.any()

df1.set_index(['PERIOD'], inplace=True)

df1 = df1.drop('Jan.-Dec. 2018')
df1 = df1.drop('Jan.-Dec. 2019')
df1 = df1.drop('Jan.-Dec. 2020')

df1 = df1.reset_index()

counts = df1.groupby("PRODUCT").size()
for i in counts.index:
    df1.PRODUCT= df1.PRODUCT.replace({i:translator.translate(i)})
df1 = df1.drop(columns=['PARTNER'])

df1 = df1.convert_dtypes()

df1.rename(columns = {'PERIOD' : 'fecha',
                      'REPORTER' : 'pais',
                      'PRODUCT' : 'producto',
                      'FLOW' : 'flow',
                      'VALUE_IN_EUROS' : 'valor en euros',
                      'QUANTITY_IN_100KG' : 'cantidad en 100Kg'}, inplace = True)

# Hay alguna columna que no se cambia al usar el metodo convert_dtypes
# por lo que hay que cambiarla a mano
df1['pais'] = df1['pais'].astype('string')

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

def convert_month_to_number(param):
    """ Utiliza una expresión regular para buscar el nombre del mes en el valor dado"""
    regex = r"^(\w+)"
    month_name = re.search(regex, param).group(1)

    # Utiliza el diccionario trad_meses para cambiar el nombre del mes por un número
    month_number = trad_meses[month_name]

    # Devuelve el valor dado con el mes en formato numérico
    return month_number + "/" + param[-4:]

# Aplica la función "convert_month_to_number" a la columna "fecha" usando la función .apply()
df1["fecha"] = df1["fecha"].apply(convert_month_to_number)

profile = ProfileReport(df1, title="Imports-Exports", html={'style':{'full_width': True}})
profile.to_notebook_iframe()

#------------------------------------------------------------------------------------------------

data5 = pd.read_csv(ruta + "\\Datasets\\Dataset5_Coronavirus_cases.txt", sep="|")

data5 = data5[['countriesAndTerritories', 'month', 'year', 'cases', 'deaths', 'continentExp',
               'Cumulative_number_for_14_days_of_COVID-19_cases_per_100000']]

data5.rename(columns = {'countriesAndTerritories' : 'pais',
                        'month' : 'mes',
                        'year' : 'año',
                        'cases' : 'casos',
                        'deaths' : 'muertes',
                        'continentExp' : 'continente',
                        'Cumulative_number_for_14_days_of_COVID-19'+
                        '_cases_per_100000' : 'incicencia_acumulada'
                        }, inplace = True)

data5['fecha'] = data5['mes'].astype(str) + '/' + data5['año'].astype(str)

data5.set_index(['continente'], inplace=True)

data5 = data5.drop('Africa')
data5 = data5.drop('America')
data5 = data5.drop('Asia')
data5 = data5.drop('Oceania')
data5 = data5.drop('Other')

data5 = data5[['pais', 'fecha', 'casos', 'muertes', 'incicencia_acumulada']]

data5['incicencia_acumulada'] = data5['incicencia_acumulada'].str.replace(',', '.').astype(float)
data5['incicencia_acumulada'] = data5['incicencia_acumulada'].fillna(0.0)

data5.set_index(['pais', 'fecha'], inplace=True)

data5['casos_mes'] = data5.groupby(['pais', 'fecha'])['casos'].sum()
data5['muertes_mes'] = data5.groupby(['pais', 'fecha'])['muertes'].sum()
data5['incicencia_media_mes'] = data5.groupby(['pais', 'fecha'])['incicencia_acumulada'].mean()

data5 = data5[['casos_mes', 'muertes_mes', 'incicencia_media_mes']]
data5 = data5.drop_duplicates()

data5 = data5.reset_index()

data5['fecha'] = data5['fecha'].astype('string')
data5['pais'] = data5['pais'].astype('string')

columns_d5 = data5.columns.tolist()
columns_d5.insert(0, columns_d5.pop(columns_d5.index('fecha')))
data5 = data5.reindex(columns = columns_d5)

profile = ProfileReport(data5, title="Coronavirus cases", html={'style':{'full_width': True}})
profile.to_notebook_iframe()
