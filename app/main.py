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
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.support import expected_conditions as EC
from utils import *

locale.setlocale(locale.LC_ALL, 'pt_pt.UTF-8')

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

def initialize_driver() -> webdriver.Firefox:
    options = webdriver.FirefoxOptions()
    options.binary_location = r"C:\Users\CS410843\AppData\Local\Mozilla Firefox\firefox.exe"
    # options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    return webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=options)
    
    
def login(driver: webdriver.Firefox, email: str, password: str) -> None:
    driver.get("https://sintegre.ons.org.br/")
    username_input = driver.find_element(By.ID, "username")
    username_input.send_keys(email)

    password_input = driver.find_element(By.ID, "password")
    password_input.send_keys(password)

    login_button = driver.find_element(By.ID, "kc-login")
    login_button.click()
    


def download_product(driver:webdriver.Firefox, products: List[dict]) -> None:
    airflow_webhook_payload:List[dict] = []
    for product in products:
        url = product["baseUrl"]
        print(url)
        driver.set_page_load_timeout(1)
        try:
            driver.get(url)
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'Download')]"))
            )
            print(f"Download iniciado para {url}")
        except Exception as e:
            print(f"Erro ao acessar {url}: {e}")

    
    
    
def teste():
    load_dotenv()
    __email = os.getenv("EMAIL")
    __password = os.getenv("PASSWORD")
    
    driver = initialize_driver()
    login(driver, __email, __password)
    sleep(5)
    produtos = get_products()
    produtos = get_url_datetime_pattern(produtos, product_date=datetime.date(2025, 1, 3))
    
    download_product(driver, produtos)
    
    # driver.get("https://sintegre.ons.org.br/sites/9/46/Produtos/479/PrevCargaDESSEM_2025-01-19.zip")

    # waits for all the files to be completed and returns the paths
    # paths = WebDriverWait(driver, 120, 1).until(every_downloads_chrome)
    print(paths)
    driver.quit()
    
    
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
    produtos = get_url_datetime_pattern(produtos, product_date=datetime.date(2025, 1, 3))
    print("iniciando downloads")
    download_product(driver, produtos)
    
    # driver.get("https://sintegre.ons.org.br/sites/9/46/Produtos/479/PrevCargaDESSEM_2025-01-19.zip")

    # waits for all the files to be completed and returns the paths
    # paths = WebDriverWait(driver, 120, 1).until(every_downloads_chrome)
    # print(paths)
    driver.quit()


def get_url_datetime_pattern(produtos, product_date:datetime.date) -> List[dict]:

    for prod in produtos:
        date_params = []
        for date_diff in prod['dateDiff']:
            if prod['dateDiffUnit'] == 'day':
                date_params.append(product_date + datetime.timedelta(days=date_diff))
            elif prod['dateDiffUnit'] == 'month':
                date_params.append(product_date.replace(day=15) + datetime.timedelta(days=(30 * date_diff)))
        url_params = [x.strftime(prod['datePattern']).encode('utf-8').decode('utf-8') if "{" not in prod['datePattern'] else elec_date_to_str(x, prod['datePattern']) for x in date_params]
        
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