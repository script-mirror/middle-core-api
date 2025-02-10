import os
import pdb
import json
import locale
import datetime
from time import sleep
from typing import List, Dict
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC

from utils import *
from airflow import trigger_airflow_dag
locale.setlocale(locale.LC_ALL, 'pt_BR.utf-8')

import time

PATH_DOWNLOAD = "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/webhook/arquivos/tmp"

def get_products():
    with open('produtos-sintegre.json', encoding="UTF8") as products_file:
        products = json.load(products_file)
    return products
    
def every_downloads_chrome(driver):
    if not driver.current_url.startswith("chrome://downloads"):
        driver.get("chrome://downloads/")
    return driver.execute_script("""
        var items = document.querySelector('downloads-manager')
            .shadowRoot.getElementById('downloadsList').items;
        if (items.every(e => e.state === "COMPLETE"))
            return items.map(e => e.fileUrl || e.file_url);
        """)

def initialize_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless")
    options.page_load_strategy = "eager"
    options.add_experimental_option("prefs", {
    "download.default_directory": PATH_DOWNLOAD,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    
def login(driver: webdriver.Chrome, email: str, password: str) -> None:
    driver.get("https://sintegre.ons.org.br/")
    username_input = driver.find_element(By.ID, "username")
    username_input.send_keys(email)

    password_input = driver.find_element(By.ID, "password")
    password_input.send_keys(password)

    login_button = driver.find_element(By.ID, "kc-login")
    login_button.click()
    


def download_products(driver:webdriver.Chrome, products: List[dict], product_date:datetime.date) -> List[dict]:
    webhook_payload:List[dict] = []
    failed:List[str] = []

    paths_download = []
    for product in products:
        url = product["baseUrl"]
        path_arquivo = f'{PATH_DOWNLOAD}{url[url.rfind("/"):]}'
        paths_download.append(path_arquivo)
        
        driver.set_page_load_timeout(1)
        try:
            driver.get(url)
            print(url)
            if '.txt' in url:
                sleep(1)
                txt:str = driver.page_source[driver.page_source.index('pre-wrap;">')+11:driver.page_source.index('</pre></body></html>')]
                with open(path_arquivo, "w", encoding="utf-8") as file:
                    file.write(txt)
                
            sleep(1)
            print(f"Download iniciado para {url}")
            webhook_payload.append({
                "origem": "botSintegre",
                "dataProduto": (product_date + (datetime.timedelta(days=product['dateDiff'][-1]))).strftime("%d/%m/%Y"),
                "nome": product["name"],
                "url": path_arquivo,
                "enviar":False
            })
            
        except Exception as e:
            failed.append(product)
        # if not os.path.exists(path_arquivo):
    return webhook_payload

    
def send_to_webhook(paths_download:List[str]) -> None:
    
    print("-"*60)
    for product in paths_download:
        if os.path.exists(product['url']):
            trigger_airflow_dag("WEEBHOOK", {"product_details":product})
        else:
            print(product)
            print('produto nao encontrado')
            print("-"*60)

def is_download_complete(download_dir):
    for filename in os.listdir(download_dir):
        if filename.endswith('.crdownload'):
            return False
    return True   

def main() -> None:
    load_dotenv()
    __email = os.getenv("EMAIL")
    __password = os.getenv("PASSWORD")
    print("init")
    driver = initialize_driver()
    print("login")
    login(driver, __email, __password)
    print("logado, pegando produtos")
    produtos = get_products()
    product_date = datetime.date(2025, 2, 6)
    produtos = get_url_datetime_pattern(produtos, product_date=product_date)
    print("iniciando downloads")

    paths_download:List[dict] = download_products(driver, produtos, product_date=product_date)
    
    timeout = 300
    start_time = time.time()

    while not is_download_complete(PATH_DOWNLOAD):
        if time.time() - start_time > timeout:
            raise Exception("O download não foi concluído dentro do tempo limite.")
        time.sleep(1)
    
    driver.quit()
    send_to_webhook(paths_download)
    # driver.get("https://sintegre.ons.org.br/sites/9/46/Produtos/479/PrevCargaDESSEM_2025-01-19.zip")

    # waits for all the files to be completed and returns the paths




def get_url_datetime_pattern(produtos, product_date:datetime.date) -> List[dict]:

    for prod in produtos:
        date_params = []
        for date_diff in prod['dateDiff']:
            if prod['dateDiffUnit'] == 'day':
                date_params.append(product_date + datetime.timedelta(days=date_diff))
            elif prod['dateDiffUnit'] == 'month':
                date_params.append(product_date.replace(day=15) + datetime.timedelta(days=(30 * date_diff)))
        url_params = [x.strftime(prod['datePattern']).encode('utf-8').decode('utf-8') if "{" not in prod['datePattern'] else elec_date_to_str(x, prod['datePattern']) for x in date_params]
        url_params = [x.upper() if "%b" in prod['datePattern'] else x for x in url_params]
        if prod['baseUrl'].format(*url_params) != "":
            prod['baseUrl'] = prod['baseUrl'].format(*url_params)
    
    return produtos

def elec_date_to_str(date:datetime.date, format:str) -> str:
    elec_date_str = ""
    elec_date = ElecData(date)
    rev = elec_date.current_revision
        
    if format == '%Y%m_REV{%Y-%m-%d} OU %Y%m_PMO':
        if rev == 0:
            elec_date_str = f"{date.strftime('%Y%m_PMO')}"
        else:
            elec_date_str = f"{date.strftime('%Y%m_REV')}{rev}"

    elif format == 'RV{%Y-%m-%d}_PMO_%B_%Y':
        elec_date_str = f"RV{rev}_PMO_{(date.strftime('%B_%Y')).title()}"
    elif format == 'RV{%Y-%m-%d}_PMO_%B%Y':
        elec_date_str = f"RV{rev}_PMO_{(date.strftime('%B%Y')).title()}"

    elif format == '%m%Y_rv{%Y-%m-%d}d%d':
        elec_date_str = f"{date.strftime('%m%Y_rv')}{rev}d{str(date.day).zfill(2)}"

    return elec_date_str
    
    

    
if __name__ == "__main__":
    main()