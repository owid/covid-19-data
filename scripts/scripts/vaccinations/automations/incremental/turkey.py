import datetime
import pytz
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import vaxutils


def main():

    url = "https://covid19asi.saglik.gov.tr/"

    # Options for Chrome WebDriver
    op = Options()
    op.add_argument("--headless")

    with webdriver.Chrome(options=op) as driver:
        driver.get(url)
        count = driver.find_element_by_class_name("count-nums1").text

    count = vaxutils.clean_count(count)

    date = str(datetime.datetime.now(pytz.timezone("Asia/Istanbul")).date())

    vaxutils.increment(
        location="Turkey",
        total_vaccinations=count,
        date=date,
        source_url=url,
        vaccine="Sinovac"
    )


if __name__ == '__main__':
    main()
