from datetime import datetime

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from vax.utils.incremental import clean_count, increment, enrich_data


def read(source: str) -> pd.Series:
    op = Options()
    op.add_argument("--headless")
    with webdriver.Chrome(options=op) as driver:
        driver.get(source)
        people_vaccinated, people_fully_vaccinated = parse_vaccinations(driver)
        date = parse_date(driver)
    return pd.Series({
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "date": date
    })


def parse_vaccinations(driver: webdriver.Chrome) -> tuple:
    people_vaccinated = clean_count(driver.find_element_by_id("vaccinated_1").text)
    people_fully_vaccinated = clean_count(driver.find_element_by_id("vaccinated_2").text)
    return people_vaccinated, people_fully_vaccinated


def parse_date(driver: webdriver.Chrome) -> str:
    elem = driver.find_element_by_class_name("tabl_vactination")
    date_str_raw = pd.read_html(elem.get_attribute('innerHTML'))[0].iloc[-1, -1]
    return datetime.strptime(date_str_raw, '*данные на %d.%m.%Y').strftime("%Y-%m-%d")


def enrich_location(ds: pd.Series):
    return enrich_data(ds, "location", "Kazakhstan")


def enrich_vaccine(ds: pd.Series):
    return enrich_data(ds, "vaccine", "Sputnik V")


def add_totals(ds: pd.Series):
    total_vaccintations = ds["people_vaccinated"] + ds["people_fully_vaccinated"]
    return enrich_data(ds, "total_vaccinations", total_vaccintations)


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(add_totals)
    )


def main():
    source = "https://www.coronavirus2020.kz/"
    data = read(source).pipe(pipeline)
    increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        date=data["date"],
        source_url=source,
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()