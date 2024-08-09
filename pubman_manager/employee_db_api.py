from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class EmployeeEditor:
    def __init__(self, username, password, driver_path):
        self.username = username
        self.password = password
        self.base_url = "https://employees.iedit.mpg.de"
        self.driver_path = driver_path

        # Set up Firefox options
        firefox_options = Options()
        firefox_options.add_argument("--headless")  # Ensure GUI is off
        firefox_options.add_argument("--no-sandbox")
        firefox_options.add_argument("--disable-dev-shm-usage")

        # Initialize the WebDriver
        self.driver = webdriver.Firefox(service=Service(self.driver_path), options=firefox_options)
        self.wait = WebDriverWait(self.driver, 10)

    def login(self):
        login_url = f"{self.base_url}/login?locale=de"
        self.driver.get(login_url)

        # Find and fill in the email field
        email_field = self.wait.until(EC.presence_of_element_located((By.ID, "email")))
        email_field.clear()
        email_field.send_keys(self.username)

        # Find and fill in the password field
        password_field = self.driver.find_element(By.ID, "password")
        password_field.clear()
        password_field.send_keys(self.password)

        # Find and click the submit button
        submit_button = self.driver.find_element(By.XPATH, "//input[@type='submit' and @value='Anmelden']")
        submit_button.click()

        # Wait for login to complete
        self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "logout")))
        print("Login successful")

    def search_employee(self, search_query):
        employees_url = f"{self.base_url}/de/employees"
        self.driver.get(employees_url)

        # Find the search input field and enter the search query
        search_field = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='search']")))
        search_field.clear()
        search_field.send_keys(search_query)
        search_field.send_keys(Keys.RETURN)

        # Wait for the results to load
        self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#employees_index_de tbody tr")))

        # Find the first entry in the list
        first_entry = self.driver.find_element(By.CSS_SELECTOR, "#employees_index_de tbody tr")
        link = first_entry.find_element(By.CSS_SELECTOR, "a[href]")
        first_entry_url = link.get_attribute("href")
        print(f"First entry URL: {first_entry_url}")
        return first_entry_url

    def run(self, search_query):
        try:
            self.login()
            self.search_employee(search_query)
        finally:
            self.driver.quit()

# Example usage
import os
username = os.getenv("USERNAME_EMPLOYEE_DB")
password = os.getenv("PASSWORD_EMPLOYEE_DB")
search_query = "Franz Roters"

# Path to your manually downloaded geckodriver
driver_path = "path/to/your/geckodriver"

editor = EmployeeEditor(username, password, driver_path)
editor.run(search_query)
