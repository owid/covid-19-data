import vaxutils
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException


def main():

    op = Options()
    op.add_argument("--headless")
    
    with webdriver.Chrome(options=op) as driver:

        url = "https://app.powerbi.com/view?r=eyJrIjoiMzg4YmI5NDQtZDM5ZC00ZTIyLTgxN2MtOTBkMWM4MTUyYTg0IiwidCI6ImFmZDBhNzVjLTg2NzEtNGNjZS05MDYxLTJjYTBkOTJlNDIyZiIsImMiOjh9"
        driver.get(url)

        # Wait for the desired element to load. If nothing is found after 25 seconds, returns.
        timeout = 25

        try:
            element_present = EC.presence_of_element_located((By.CLASS_NAME, "value"))
            data = WebDriverWait(driver, timeout).until(element_present).text
        except TimeoutException:
            return
        count = vaxutils.clean_count(data)

        try:
            element_present = EC.presence_of_element_located((By.CLASS_NAME, "title"))
            data = WebDriverWait(driver, timeout).until(element_present).text
        except TimeoutException:
            return
        date = vaxutils.clean_date(data, "%m/%d/%Y %H:%M:%S %p")

        vaxutils.increment(
            location="Italy",
            total_vaccinations=count,
            date=date,
            source_url=url,
            vaccine="Pfizer/BioNTech"
        )


if __name__ == "__main__":
    main()
