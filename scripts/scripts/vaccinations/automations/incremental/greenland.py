import locale
from datetime import datetime

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import vaxutils


def read(source: str) -> pd.Series:
    return connect_parse_data(source)


def connect_parse_data(source: str) -> pd.Series:
    op = Options()
    op.add_argument("--headless")

    with webdriver.Chrome(options=op) as driver:
        driver.implicitly_wait(20)
        driver.get(source)
        # Get date
        date = parse_date(driver)
        # Get and load iframe
        source_iframe = parse_iframe_url(driver)
        driver.get(source_iframe)
        # Sanity check
        _sanity_checks(driver)
        # Get doses
        dose_1, dose_2 = parse_doses(driver)
    return pd.Series({
        "people_vaccinated": dose_1,
        "people_fully_vaccinated": dose_2,
        "date": date
    })


def parse_iframe_url(driver: webdriver.Chrome) -> str:
    iframe_url = driver.find_element_by_class_name("vaccine").get_attribute("src")
    return iframe_url


def parse_doses(driver: webdriver.Chrome) -> tuple:
    doses = driver.find_element_by_class_name("igc-graph-group").find_elements_by_tag_name("text")
    dose_1, dose_2 = [vaxutils.clean_count(dose.text) for dose in doses]
    return dose_1, dose_2


def parse_date(driver: webdriver.Chrome) -> str:
    text = driver.find_element_by_class_name("content").text
    return datetime.strptime(text, "Opdateret: %d. %B %Y").strftime("%Y-%m-%d")


def _sanity_checks(driver: webdriver.Chrome):
    elems = driver.find_elements_by_class_name("igc-legend-label")
    labels = [e.text for e in elems]
    if labels[0] != "Fået 1. vaccine" or labels[1] != "Modtaget 2. vaccine*":
        raise Exception("First graph structure has changed. Consider manually checking the axis labels in the browser.")


def enrich_source(ds: pd.Series, source: str) -> pd.Series:
    return vaxutils.enrich_data(ds, "source_url", source)


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "vaccine", "Pfizer/BioNTech")


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "location", "Greenland")


def add_totals(ds: pd.Series) -> pd.Series:
    total_vaccinations = ds['people_vaccinated'] + ds['people_fully_vaccinated']
    return vaxutils.enrich_data(ds, 'total_vaccinations', total_vaccinations)


def pipeline(ds: pd.Series, source: str) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source, source)
        .pipe(add_totals)
    )


def main():
    locale.setlocale(locale.LC_TIME, "da_DK")
    source = "https://corona.nun.gl/emner/statistik/antal_vaccinerede"
    data = read(source).pipe(pipeline, source)
    vaxutils.increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()
