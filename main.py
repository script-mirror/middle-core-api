from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from webdriver_manager.firefox import GeckoDriverManager
from time import sleep

firefox_options = webdriver.FirefoxOptions()
firefox_options.binary_location = r""
driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=firefox_options)
# firefox_options.add_argument("--headless")
firefox_options.add_argument("--no-sandbox")
firefox_options.add_argument("--disable-dev-shm-usage")

# Abre a página de login
driver.get("https://sintegre.ons.org.br/")

# Preenche o campo de username
username_input = driver.find_element(By.ID, "username")
username_input.send_keys("")
# Preenche o campo de password
password_input = driver.find_element(By.ID, "password")
password_input.send_keys("")

# Clica no botão de login
login_button = driver.find_element(By.ID, "kc-login")
login_button.click()
sleep(5)

driver.get("https://sintegre.ons.org.br/sites/9/46/Produtos/479/PrevCargaDESSEM_2025-01-19.zip")
driver.quit()

print("Login realizado com sucesso")