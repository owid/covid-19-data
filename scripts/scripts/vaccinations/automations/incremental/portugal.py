import re
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import vaxutils


def main():

    url = "https://esriportugal.maps.arcgis.com/apps/opsdashboard/index.html#/acf023da9a0b4f9dbb2332c13f635829"

    # Options for Chrome WebDriver
    op = Options()
    # op.add_argument("--headless")

    with webdriver.Chrome(options=op) as driver:

        driver.get(url)
        time.sleep(4)

        for box in driver.find_elements_by_class_name("indicator-top-text"):

            if "Total de Vacinas Administradas" in box.text:
                count_text = box.find_element_by_xpath("..").text

            elif "Dados relativos ao boletim da DGS de" in box.text:
                date_text = box.find_element_by_xpath("..").text

    count = re.search(r"\n([\d\s]+$)", count_text).group(1)
    count = vaxutils.clean_count(count)

    date = re.search(r"\n([\d/]+$)", date_text).group(1)
    date = vaxutils.clean_date(date, "%d/%m/%Y")

    vaxutils.increment(
        location="Portugal",
        total_vaccinations=count,
        date=date,
        source_url=url,
        vaccine="Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
