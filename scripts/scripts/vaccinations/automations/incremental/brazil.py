import datetime
import time
import pytz
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import vaxutils


def main():

    # Options for Chrome WebDriver
    op = Options()
    op.add_argument("--headless")

    with webdriver.Chrome(options=op) as driver:

        url = "https://datastudio.google.com/embed/u/0/reporting/2f2537fa-ac23-4f08-8741-794cdbedca03/page/CPFTB"
        driver.get(url)
        time.sleep(5)
        for elem in driver.find_elements_by_class_name("kpimetric"):
            if "Vacinados" in elem.text:
                total_vaccinations = elem.find_element_by_class_name("valueLabel").text

    total_vaccinations = vaxutils.clean_count(total_vaccinations)
    people_vaccinated = total_vaccinations

    date = str(datetime.datetime.now(pytz.timezone("Brazil/East")).date())

    vaxutils.increment(
        location="Brazil",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        date=date,
        source_url="https://coronavirusbra1.github.io/",
        vaccine="Oxford/AstraZeneca, Sinovac"
    )


if __name__ == "__main__":
    main()
