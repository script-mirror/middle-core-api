import os
import pdb
import json
import time
import locale
import datetime
import argparse
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

import logging
logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s:\t%(asctime)s\t %(name)s.py:%(lineno)d\t %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[
                        logging.StreamHandler()
                    ])

load_dotenv()

PATH_DOWNLOAD = os.getenv("PATH_DOWNLOAD")

def get_products():
    with open('produtos-sintegre.json', encoding="UTF8") as products_file:
        products = json.load(products_file)
    return products
    

def initialize_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
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
        
        driver.set_page_load_timeout(10)
        try:
            print('\n')
            driver.get(url)
            logging.info(url)
            if '.txt' in url:
                time.sleep(1)
                txt:str = driver.page_source[driver.page_source.index('pre-wrap;">')+11:driver.page_source.index('</pre></body></html>')]
                with open(path_arquivo, "w", encoding="utf-8") as file:
                    file.write(txt)
                
            time.sleep(1)
            logging.info(f"\033[92mDownload iniciado para {product['name']}\033[0m")
            webhook_payload.append({
                "function_name":product['funcName'],
                "product_details":
                {"origem": "botSintegre",
                "dataProduto": (product_date + (datetime.timedelta(days=product['dateDiff'][-1]))).strftime("%d/%m/%Y"),
                "nome": product["name"],
                "url": path_arquivo,
                "enviar":False}
            })
            
        except Exception as e:
            logging.warning(f"\033[91mErro no produto {product['name']}\033[0m")
            
            failed.append(product)
        # if not os.path.exists(path_arquivo):
    return webhook_payload

    
def send_to_webhook(airflow_products:List[str]) -> None:
    logging.info("-"*60)
    pdb.set_trace()
    for product in airflow_products:
        if os.path.exists(product['product_details']['url']):
            res = trigger_airflow_dag("WEBHOOK", product)
            if res:
                logging.info(f"Produto {product['product_details']['nome']} enviado para o webhook")
            else:
                logging.info(f"Erro ao enviar para o webhook {product['product_details']['nome']}")
        else:
            logging.info(product)
            logging.info('produto nao encontrado')
            logging.info("-"*60)


def is_download_complete(download_dir:str) -> bool:
    for filename in os.listdir(download_dir):
        if filename.endswith('.crdownload'):
            return False
    return True   

def get_url_datetime_pattern(products:List[dict], product_date:datetime.date) -> List[dict]:

    for prod in products:
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
    
    return products

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

def execute_bot(product_date:datetime.date = datetime.date.today(), trigger_webhook:bool = False, download_timeout:int = 300) -> None:
    __email = os.getenv("EMAIL")
    __password = os.getenv("PASSWORD")

    logging.info("Limpando pasta de produtos...")
    os.system(f'rm -r {PATH_DOWNLOAD}/*')
    
    driver = initialize_driver()
    logging.info("Realizando login")
    login(driver, __email, __password)

    produtos = get_url_datetime_pattern(get_products(), product_date=product_date)

    logging.info("Iniciando downloads")

    airflow_products:List[dict] = download_products(driver, produtos, product_date=product_date)

    start_time = time.time()
    while not is_download_complete(PATH_DOWNLOAD):
        logging.info(f"Download em andamento... {time.time() - start_time:.2f}s")
   
        if time.time() - start_time > download_timeout:
            raise Exception("O download não foi concluído dentro do tempo limite.")
        time.sleep(1)
    driver.quit()
    if trigger_webhook:
        logging.info("Enviando para o webhook")
        send_to_webhook(airflow_products)
    logging.info("Fim")





def main() -> None:
    print("python main.py --help\n")
    parser = argparse.ArgumentParser(description='Execute o bot com os seguintes parametros.')
    parser.add_argument('--product_date', type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d').date(), default=datetime.date.today(), help='Product date no formato YYYY-MM-DD')
    parser.add_argument('--no_trigger', action='store_false', help='Nao triga webhook')
    parser.add_argument('--download_timeout', type=int, default=300, help='Download timeout maximo em segundos')

    args = parser.parse_args()
    execute_bot(product_date=args.product_date, trigger_webhook=args.trigger_webhook, download_timeout=args.download_timeout)

    
    

    
if __name__ == "__main__":

    # send_to_webhook(    [{
    #     "function_name": "arquivo_acomph",
    #     "product_details": {
    #         "origem": "botSintegre",
    #         "dataProduto": "09/02/2025",
    #         "nome": "Acomph",
    #         "url": "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/webhook/arquivos/tmp2/ACOMPH_09.02.2025.xls",
    #         "enviar": True
    #     }
    # }])
    main()