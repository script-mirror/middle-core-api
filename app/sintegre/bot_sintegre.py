import os
import pdb
import json
import time
import base64
import locale
import datetime
from typing import List, Dict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from app.sintegre.utils import *
from app.airflow.service import trigger_airflow_dag
locale.setlocale(locale.LC_ALL, 'pt_BR.utf-8')
import logging
from app.core.config import settings
import requests
# from .service import ProductService


# product_service = ProductService()

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s:\t%(asctime)s\t %(name)s.py:%(lineno)d\t %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[
                        logging.StreamHandler()
                    ])



def get_products(product_id):
    with open('produtos-sintegre.json', encoding="UTF8") as products_file:
        products = json.load(products_file)
    if product_id:
        return [x for x in products if x['id'] == product_id]
    return products
    

def initialize_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.page_load_strategy = "eager"
    options.add_experimental_option("prefs", {
    "download.default_directory": "/tmp",
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
    


def download_products(driver:webdriver.Chrome, products: List[dict], product_date:datetime.date) -> Dict[str, List[dict]]:
    webhook_payload:List[dict] = []
    failed:List[str] = []

    paths_download = []
    for product in products:
        url = product["baseUrl"]
        
        path_arquivo = f'/tmp/{url[url.rfind("/")+1:]}'
        try:
            os.remove(path_arquivo)
            logging.info(f"{path_arquivo} removido")
        except:
            pass
        paths_download.append(path_arquivo)
        
        driver.set_page_load_timeout(20)
        try:
            driver.get(url)
            logging.info(url)
            if '.txt' in url:
                time.sleep(1)
                txt:str = driver.page_source[driver.page_source.index('pre-wrap;">')+11:driver.page_source.index('</pre></body></html>')]
                with open(path_arquivo, "w", encoding="utf-8") as file:
                    file.write(txt)
            filename = path_arquivo[path_arquivo.rfind("/")+1:]
            logging.info(f"Download iniciado para {filename}")
            time.sleep(2)

            webhook_payload.append({
                "function_name":product['funcName'],
                "product_details":
                {"origem": "botSintegre",
                "dataProduto": (product_date + (datetime.timedelta(days=product['dateDiff'][-1]))).strftime("%d/%m/%Y"),
                "nome": product["name"],
                "base64": path_arquivo,
                "filename": filename,
                "enviar":True}
            })
            
        except Exception as e:
            logging.warning(f"Erro no produto {product['name']}")
            failed.append(product)
    return {"success":webhook_payload, "failed":failed}

    
def send_to_webhook(airflow_products:List[str]) -> None:
    for product in airflow_products:
        is_new = requests.post("http://localhost:8000/api/v2/bot-sintegre/verify", json={"nome":product['product_details']['nome'], "filename":product['product_details']['filename']}).json()

        if is_new:
            res = trigger_airflow_dag("WEBHOOK", product)
        else:
            logging.info("produto repetido")
        if res.status_code == 200:
            print("Produto enviado para o webhook")
        else:
            logging.info(f"Erro ao enviar para o webhook {product['product_details']['nome']}")


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

    while date.weekday() != 5:
        date += datetime.timedelta(days=1)

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

def trigger_bot(product_date:datetime.date, product_id:int=None, trigger_webhook:bool=True, download_timeout:int = 300) -> None:
    __email = settings.sintegre_email
    __password = settings.sintegre_password
    
    driver = initialize_driver()
    logging.info("Realizando login")
    login(driver, __email, __password)

    produtos = get_url_datetime_pattern(get_products(product_id), product_date=product_date)

    logging.info("Iniciando downloads")

    airflow_products:List[dict] = download_products(driver, produtos, product_date=product_date)
    products_to_remove = []
    for product in airflow_products['success']:
        url = product['product_details']['base64']
        if url != "":        
            try:
                with open(url, "rb") as f:
                    data = f.read()
            except:
                products_to_remove.append(product)
                continue
            file_base64 = base64.b64encode(data).decode('utf-8')
            product['product_details']['base64'] = file_base64
            product['product_details']['filename'] = url[url.rfind("/")+1:]

    for product in products_to_remove:
        airflow_products['success'].remove(product)
        
    start_time = time.time()
    while not is_download_complete("/tmp"):
        logging.info(f"Download em andamento... {time.time() - start_time:.2f}s")
   
        if time.time() - start_time > download_timeout:
            raise Exception("O download não foi concluído dentro do tempo limite.")
        time.sleep(1)
    driver.quit()

    if trigger_webhook:
        logging.info("Enviando para o webhook")
        send_to_webhook(airflow_products['success'])

    return {'failed':airflow_products['failed']}

if __name__ == "__main__":
    produtos = get_products()
    for produto in produtos:
        print(f"""VALUES('{produto["name"]}'),""")