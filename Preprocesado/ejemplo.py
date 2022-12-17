"""ejemplo"""

import os
import pandas as pd
ruta = os.getcwd()

archivos = [ruta + '\\Datasets\\2018datosmensualesdelpaneldeconsumoalimentarioenhogares_tcm30-520451_tcm30-520451.xlsx',
        ruta + '\\Datasets\\2019datosmensualesdelpaneldeconsumoalimentarioenhogares_tcm30-5204501_tcm30-520450.xlsx',
        ruta + '\\Datasets\\2020-datos-mensuales-panel-hogares-ccaa-rev-nov2021_tcm30-540244.xlsx']
poblacion = ruta + '\\Datasets\\2915bsc.csv'

# Indice para avanzar en el fichero de forma inversa
i = 2

lista_mes = []
lista_mes_2020 = []
meses = {'Enero':'01', 'Febrero':'02', 'Marzo':'03', 'Abril':'04', 'Mayo':'05', 'Junio':'06', 'Julio':'07',
         'Agosto':'08', 'Septiembre':'09', 'Octubre':'10', 'Noviembre':'11', 'Diciembre':'12'}
first = True
pob = pd.read_csv(poblacion, sep=';', encoding='latin-1')
products = pd.DataFrame(columns = ['CONSUMO EN HOGARES'])
mes_aux = pd.DataFrame(columns = ['Producto', 'Precio AND (€/Kg)', 'Precio MAD (€/Kg)', 'Precio CLM (€/Kg)', 'Precio CYL (€/Kg)',
             'Precio CAT (€/Kg)', 'Valor AND (miles €)', 'Valor MAD (miles €)', 'Valor CLM (miles €)', 
             'Valor CYL (miles €)', 'Valor CAT (miles €)', 'Volumen AND (miles kg)', 'Volumen MAD (miles kg)',
             'Volumen CLM (miles kg)', 'Volumen CYL (miles kg)', 'Volumen CAT (miles kg)', 'Poblacion AND',
             'Poblacion MAD', 'Poblacion CLM', 'Poblacion CYL', 'Poblacion CAT'])

# Bucle para la limpieza y obtención de los datos de los datasets originales
for file in archivos:
  año = file[0:4]
  condition = True
  
  # Diferenciamos entre 2018/19 y 2020 al cambiar de formato para nombrar las hojas del excel
  if año == '2020':
    meses = {k.lower(): v for k, v in meses.items()}
    condition = False

  for mes in meses:
    print(mes)
    xl = pd.read_excel(file, sheet_name = mes)
    mes_2020 = mes_aux = pd.DataFrame(columns = ['Producto', 'Fecha', 'Precio AND (€/Kg)', 
                                                 'Precio MAD (€/Kg)', 'Valor AND (miles €)', 
                                                 'Valor MAD (miles €)', 'Volumen AND (miles kg)', 
                                                 'Volumen MAD (miles kg)'])
    # Cogemos los productos que nos interesan
    if mes == 'Enero' and first:
      products['CONSUMO EN HOGARES'] = xl.iloc[420:465]['CONSUMO EN HOGARES']
      first = False
    
    # Juntamos el dataset base con la lista de productos con un merge para obtener solo los datos de interés
    first_month = pd.merge(products, xl, on='CONSUMO EN HOGARES', how='left')
    mes_aux[['Producto', 'Precio AND (€/Kg)', 'Precio MAD (€/Kg)', 'Precio CLM (€/Kg)', 'Precio CYL (€/Kg)',
             'Precio CAT (€/Kg)', 'Valor AND (miles €)', 'Valor MAD (miles €)', 'Valor CLM (miles €)', 
             'Valor CYL (miles €)', 'Valor CAT (miles €)', 'Volumen AND (miles kg)', 'Volumen MAD (miles kg)',
             'Volumen CLM (miles kg)', 'Volumen CYL (miles kg)', 'Volumen CAT (miles kg)']] = first_month[['CONSUMO EN HOGARES', 
             'Unnamed: 40', 'Unnamed: 46', 'Unnamed: 52', 'Unnamed: 64', 'Unnamed: 10',
             'Unnamed: 41', 'Unnamed: 47', 'Unnamed: 53', 'Unnamed: 65', 'Unnamed: 11',
             'Unnamed: 42', 'Unnamed: 48', 'Unnamed: 54', 'Unnamed: 66', 'Unnamed: 12',]]
    
    mes_aux['Fecha'] = meses[mes]+'/'+año

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