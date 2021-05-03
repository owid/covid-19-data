import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from vax.utils.incremental import enrich_data, increment, clean_date, clean_count


def read(source: str, source_old: str) -> pd.Series:
    return connect_parse_data(source, source_old)


def connect_parse_data(source: str, source_old: str) -> pd.Series:
    op = Options()
    op.add_argument("--headless")

    with webdriver.Chrome(options=op) as driver:
        driver.get(source)
        time.sleep(5)

        total_vaccinations = driver.find_element_by_id("counter1").text
        people_vaccinated = driver.find_element_by_id("counter2").text
        people_fully_vaccinated = driver.find_element_by_id("counter3").text
        
        driver.get(source_old)
        time.sleep(5)

        # Sanity check
        total_vaccinations_old = driver.find_element_by_id("counter1").text
        if total_vaccinations != total_vaccinations_old:
            raise ValueError("Both dashboards may not be synced and hence may refer to different timestamps. Consider"
                             "Introducing the timestamp manually.")
        date = driver.find_element_by_id("pupdateddate").text
        date = clean_date(date.replace("Updated ", ""), "%d %b, %Y")

    data = {
        "total_vaccinations": clean_count(total_vaccinations),
        "people_vaccinated": clean_count(people_vaccinated),
        "people_fully_vaccinated": clean_count(people_fully_vaccinated),
        "date": date,
    }
    return pd.Series(data=data)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Qatar")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Moderna, Pfizer/BioNTech")


def enrich_source(ds: pd.Series, source: str) -> pd.Series:
    return enrich_data(ds, "source_url", source)


def pipeline(ds: pd.Series, source: str) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source, source)
    )


def main():
    source = "https://covid19.moph.gov.qa/EN/Pages/Vaccination-Program-Data.aspx"
    source_old = "https://covid19.moph.gov.qa/EN/Pages/default.aspx"
    data = read(source, source_old).pipe(pipeline, source)
    increment(
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
