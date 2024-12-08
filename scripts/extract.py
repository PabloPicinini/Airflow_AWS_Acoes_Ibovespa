from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def download_pregao():
    # Configurar as opções do Chrome
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Executar em modo headless
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    prefs = {"download.default_directory": "/home/airflow/Documentos/airflowFIAP/dags/ibov_techchallenge_dois/csv/",
            "download.prompt_for_download": False,
            "directory_upgrade": True,
            "safebrowsing.enabled": True}

    chrome_options.add_experimental_option("prefs", prefs)

    # Caminho para o ChromeDriver
    chrome_service = Service()

    # Iniciar o WebDriver
    driver = webdriver.Chrome(service=chrome_service, options=chrome_options)

    # Navegar para a página desejada
    url = 'https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br'
    driver.get(url)

    # Esperar a página carregar
    wait = WebDriverWait(driver, 20)

    # Encontrar o botão de download e clicar nele
    download_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//a[contains(text(), "Download")]')))
    download_button.click()

    # Esperar o download completar
    time.sleep(2)

    # Fechar o WebDriver
    driver.quit()
