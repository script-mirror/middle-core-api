import os
import pdb
import json
import datetime
from time import sleep
from typing import List, Dict
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.firefox import GeckoDriverManager
from utils import *


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
    


def download_product(driver:webdriver.Firefox, products: List[dict], product_date:datetime.date) -> None:
    airflow_webhook_payload:List[dict] = []
    for product in products:
        url = product.get("url")
    driver.get(url)
    
    
    
def teste():
    load_dotenv()
    __email = os.getenv("EMAIL")
    __password = os.getenv("PASSWORD")
    
    driver = initialize_driver()
    login(driver, __email, __password)
    sleep(5)
    produtos = get_products()
    download_product(driver, produtos, datetime.date(2025, 1, 19))
    
    driver.get("https://sintegre.ons.org.br/sites/9/46/Produtos/479/PrevCargaDESSEM_2025-01-19.zip")

    # waits for all the files to be completed and returns the paths
    paths = WebDriverWait(driver, 120, 1).until(every_downloads_chrome)
    print(paths)
    driver.quit()
    
    
def main() -> None:
    produtos = get_products()
    
    get_url_datetime_pattern(produtos, product_date=datetime.date(2025, 1, 19))
    
    return None


def get_url_datetime_pattern(produtos, product_date:datetime.date) -> str:
    elec_formats = ['%Y%m_REV{%Y-%m-%d} OU %Y%m_PMO', 'RV{%Y-%m-%d}_PMO_%B_%Y', '%m%Y_rv{%Y-%m-%d}%d', '%Y%m_REV{%Y-%m-%d}']
    default_formats = ['', '%d.%m.%Y', '%B_%Y', '%d%mM%Y', '%Y-%m-%d', '%d_%m_%Y',  '%Y%m%d', '%d%m%Y']
    print('-'*50)
    for prod in produtos:
        date_params = []
        for date_diff in prod['dateDiff']:
            if prod['dateDiffUnit'] == 'day':
                date_params.append(product_date + datetime.timedelta(days=date_diff))
            elif prod['dateDiffUnit'] == 'month':
                product_date.day = 1
                product_date.month = product_date.month + date_diff
                date_params.append(product_date + datetime.timedelta(days=date_diff))
        url_params = [x.strftime(prod['datePattern']) if prod['datePattern'] not in elec_formats else elec_date_to_str(x, prod['datePattern']) for x in date_params]
        
        print(prod['baseUrl'].format(*url_params))
        print('-'*50)
    
    pdb.set_trace()

    abc = datetime.datetime.now()
    datetime.datetime.strftime()
    abc.strftime('%B')
    return ""

def elec_date_to_str(date:datetime.date, format:str) -> str:
    elec_date_str = ""
    elec_date = ElecData(date)
    rev = elec_date.current_revision
    
    if format == '%Y%m_REV{%Y-%m-%d}':
        elec_date_str = f"{date.strftime('%Y%m_REV')}{rev}"
        
    elif format == '%Y%m_REV{%Y-%m-%d} OU %Y%m_PMO':
        if rev == 0:
            elec_date_str = f"{date.strftime('%Y%m_PMO')}"
        else:
            elec_date_str = f"{date.strftime('%Y%m_REV')}{rev}"

    elif format == 'RV{%Y-%m-%d}_PMO_%B_%Y':
        elec_date_str = f"RV{rev}_PMO_{date.strftime('%B_%Y')}"

    elif format == '%m%Y_rv{%Y-%m-%d}d%d':
        elec_date_str = f"{date.strftime('%m%Y_rv')}{rev}d{str(date.day).zfill(2)}"

    return elec_date_str
    
    

    
if __name__ == "__main__":
    main()