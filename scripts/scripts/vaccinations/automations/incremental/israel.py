import datetime
import time
import pytz
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import vaxutils


def main():

    url = "https://datadashboard.health.gov.il/COVID-19/general"

    # Options for Chrome WebDriver
    op = Options()
    op.add_argument("--headless")

    with webdriver.Chrome(options=op) as driver:

        driver.get(url)
        time.sleep(2)

        for counter in driver.find_elements_by_class_name("title-header"):
            if "מתחסנים מנה ראשונה" in counter.text:
                count = counter.find_element_by_class_name("total-amount").text
                count = vaxutils.clean_count(count)
                break

    date = str(datetime.datetime.now(pytz.timezone("Asia/Jerusalem")).date())

    vaxutils.increment(
        location="Israel",
        total_vaccinations=count,
        date=date,
        source_url=url,
        vaccine="Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
