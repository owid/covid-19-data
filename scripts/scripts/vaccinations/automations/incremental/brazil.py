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
            if "1ª Dose" in elem.text:
                people_vaccinated = elem.find_element_by_class_name("valueLabel").text
            elif "2ª Dose" in elem.text:
                people_fully_vaccinated = elem.find_element_by_class_name("valueLabel").text

    people_vaccinated = vaxutils.clean_count(people_vaccinated)
    people_fully_vaccinated = vaxutils.clean_count(people_fully_vaccinated)
    total_vaccinations = people_vaccinated + people_fully_vaccinated

    date = str(datetime.datetime.now(pytz.timezone("Brazil/East")).date())

    vaxutils.increment(
        location="Brazil",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url="https://coronavirusbra1.github.io/",
        vaccine="Oxford/AstraZeneca, Sinovac"
    )


if __name__ == "__main__":
    main()
