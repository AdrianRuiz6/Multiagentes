# pylint: disable=C0103
'''
Clase utilizada para descargar los archivos necesarios tanto de Google Drive como con web scrapping.
'''
import logging
import time
import gdown
from selenium import webdriver
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_folder_drive(link_folder):
    '''
    Recibe como parámetro el enlace de una carpeta de Google Drive y utiliza la
    librería gdown para descargarla. Si ocurre algún error durante la
    descarga, se imprime un mensaje de error y se cambia el valor de la
    variable error a True. Al final de la función, se devuelve el valor de error.
    '''
    error = False
    try:
        gdown.download_folder(link_folder, quiet=True, use_cookies=False)
    # pylint: disable=W0702
    except:
        logging.debug("No se descargaron los archivos de Google Drive.")
        error = True
    return error

# pylint: disable=W0702
def extract_web_scrapping():
    '''
    Accede a un servidor de Chrome, acepta la cookies y descarga 3 archivos.
    '''
    error = False
    # Aquí se crea una instancia de un controlador de navegador que se ejecuta en una máquina
    # remota dentro de la cual se señala la direccion del servidor del hub de Selenium y
    # la configuración de esta instancia remota, que es de Chrome. Finalmente se pone en pantalla
    # completa este navegador.
    caps = webdriver.DesiredCapabilities.CHROME
    driver = webdriver.Remote(
        'http://localhost:4444',
        desired_capabilities=caps
    )

    # Se abre la página del dataset y se esperan dos segundos para darle tiempo a cargarse.
    # pylint: disable=C0301
    link_web = "https://www.mapa.gob.es/es/alimentacion/temas/consumo-tendencias/panel-de-consumo-alimentario/series-anuales/default.aspx"
    try:
        driver.get(link_web)
    except:
        logging.debug("No se pudo abrir la web.")
        error = True
        return error
    time.sleep(4)

    # Se busca el elemento del botón aceptar de las cookies y una vez localizado se clicka aceptándolas así.
    try:
        cookies_button = driver.find_element("xpath","/html/body/div[2]/div/span[3]/a")
        cookies_button.click()
    except:
        logging.debug("No se pudo pulsar el botón de cookies.")
        error = True
        return error

    # Se recopila el xpath del elemento que contiene el link de descarga que nos interesa de cada archivo y se declara el nombre
    # del archivo que va a ser descargado.
    xpath_2018 = "/html/body/div[4]/div/div/div[2]/div/div[2]/div[2]/div/div/div[2]/div/div/div/table[2]/tbody/tr[5]/td[3]/a"
    name_file_2018 = "mensual_CCAA_2018.xlsx"
    error = download_file_link(driver, error, xpath_2018, name_file_2018)

    xpath_2019 = "/html/body/div[4]/div/div/div[2]/div/div[2]/div[2]/div/div/div[2]/div/div/div/table[2]/tbody/tr[4]/td[3]/a"
    name_file_2019 = "mensual_CCAA_2019.xlsx"
    error = download_file_link(driver, error, xpath_2019, name_file_2019)

    xpath_2020 = "/html/body/div[4]/div/div/div[2]/div/div[2]/div[2]/div/div/div[2]/div/div/div/table[2]/tbody/tr[3]/td[3]/a"
    name_file_2020 = "mensual_CCAA_2020.xlsx"
    error = download_file_link(driver, error, xpath_2020, name_file_2020)

    # Se cierra la instancia de Chrome.
    driver.quit()
    # Se devuelve la variable 'error' para que el main reconozca si ha habido uno o no e informar adecuadamente.
    return error

def download_file_link(driver, error, xpath, name_file):
    '''
    Dado el driver para navegar por la web encuentra el elemento de la pagina
    que se quiere descargar gracias al xpath proporcionado, saca su link de
    descarga y escribe su contenido en la ruta especificada con el nombre
    especificado en la variable 'name_file'. Si se produce un error se informa
    y se cambia el valor de la variable 'error' a True.
    '''

    try:
        mensual_CCAA_2020_link = driver.find_element("xpath", xpath)
        file_url = mensual_CCAA_2020_link.get_attribute('href')
        response = requests.get(file_url, timeout=10)
        with open(f'Datasets_mineria/{name_file}', 'wb') as f:
            f.write(response.content)
    except:
        # pylint: disable=W1203
        logging.debug(f"No se pudo descargar '{name_file}'.")
        error = True
    return error
