import gdown
import time
from selenium import webdriver

def extract_folder_drive(link_folder):
    # Descarga una carpeta de Google Drive dado el enlace de esta.
    gdown.download_folder(link_folder, quiet=True, use_cookies=False)

def extract_web_scrapping():
    # Cargando opciones del navegador
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument("--window-size=1920,1080")
    options.add_experimental_option("detach", True)
    options.add_argument('--headless')
    prefs = {'download.default_directory' : 'G:\\Mi unidad\\ADRIÁN\\COMPARTIDO\\Universidad\\5º CURSO\\1º cuatrimestre\\Multiagentes\\Laboratorio\\Trabajo\\Multiagentes_app\\Datasets_mineria'}
    options.add_experimental_option('prefs', prefs)
    driver = webdriver.Chrome(options=options)

    # Abriendo la página del dataset.
    try:
        driver.get("https://www.mapa.gob.es/es/alimentacion/temas/consumo-tendencias/panel-de-consumo-alimentario/series-anuales/default.aspx")
    except:
        print("ERROR: No se pudo abrir la web.")
    time.sleep(2)

    # Buscando y pulsando el botón de aceptar las cookies de la página.
    try:
        cookies_button = driver.find_element("xpath","/html/body/div[2]/div/span[3]/a")
        cookies_button.click()
    except:
        print("ERROR: No se pudo pulsar el botón de cookies.")

    # Buscando y pulsando el botón de descarga de los archivos que queremos.
    try:
        mensual_CCAA_2018 = driver.find_element("xpath","/html/body/div[4]/div/div/div[2]/div/div[2]/div[2]/div/div/div[2]/div/div/div/table[2]/tbody/tr[5]/td[3]/a")
        mensual_CCAA_2018.click()
    except:
        print("ERROR: No se pudo descargar 'Datos mensuales por CCAA 18'.")

    try:
        mensual_CCAA_2019 = driver.find_element("xpath","/html/body/div[4]/div/div/div[2]/div/div[2]/div[2]/div/div/div[2]/div/div/div/table[2]/tbody/tr[4]/td[3]/a")
        mensual_CCAA_2019.click()
    except:
        print("ERROR: No se pudo descargar 'Datos mensuales por CCAA 19'.")

    try:
        mensual_CCAA_2020 = driver.find_element("xpath","/html/body/div[4]/div/div/div[2]/div/div[2]/div[2]/div/div/div[2]/div/div/div/table[2]/tbody/tr[3]/td[3]/a")
        mensual_CCAA_2020.click()
    except:
        print("ERROR: No se pudo descargar 'Datos mensuales por CCAA 20'.")

    # Cerrando el navegador.
    time.sleep(5)
    driver.quit()