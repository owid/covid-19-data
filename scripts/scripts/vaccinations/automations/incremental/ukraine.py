import vaxutils
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time


def read(source: str) -> pd.Series:
    return connect_parse_data(source)


def connect_parse_data(source: str) -> pd.Series:
    op = Options()
    op.add_argument("--headless")

    with webdriver.Chrome(options=op) as driver:
        driver.get(source)
        time.sleep(10)

        date = driver.find_element_by_class_name("as_of").find_element_by_tag_name("span").text
        date = vaxutils.clean_date(date, "%d.%m.%Y")

        for elem in driver.find_elements_by_class_name("counter_block"):
            if "1 ДОЗУ" in elem.text:
                people_vaccinated = elem.find_element_by_tag_name("h2").text
            if "2 ДОЗИ" in elem.text:
                people_fully_vaccinated = elem.find_element_by_tag_name("h2").text

    data = {
        "people_vaccinated": vaxutils.clean_count(people_vaccinated),
        "people_fully_vaccinated": vaxutils.clean_count(people_fully_vaccinated),
        "date": date,
    }
    return pd.Series(data=data)


def add_totals(ds: pd.Series) -> pd.Series:
    total_vaccinations = ds["people_vaccinated"] + ds["people_fully_vaccinated"]
    return vaxutils.enrich_data(ds, "total_vaccinations", total_vaccinations)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "location", "Ukraine")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "vaccine", "Oxford/AstraZeneca")


def enrich_source(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "source_url", "https://vaccination.covid19.gov.ua/")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds.pipe(add_totals)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://vaccination.covid19.gov.ua/"
    data = read(source).pipe(pipeline)
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
