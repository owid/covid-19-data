import os
import re
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from vax.utils.incremental import enrich_data, increment, clean_count, clean_date


def read(source: str) -> pd.Series:
    return connect_parse_data(source)


def connect_parse_data(source: str) -> pd.Series:
    op = Options()
    op.add_argument("--headless")

    with webdriver.Chrome(options=op) as driver:
        driver.get(source)
        time.sleep(10)

        date = re.search(r"Fecha de corte : ([\d/]{10})", driver.page_source).group(1)

        for block in driver.find_elements_by_class_name("unselectable"):
            if block.get_attribute("aria-label") == "Dosis aplicadas Card":
                total_vaccinations = clean_count(block.find_element_by_class_name("value").text)
            elif block.get_attribute("aria-label") == "Segundas dosis aplicadas Card":
                people_fully_vaccinated = clean_count(block.find_element_by_class_name("value").text)

    people_vaccinated = total_vaccinations - people_fully_vaccinated

    return pd.Series({
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "date": clean_date(date, "%d/%m/%Y")
    })


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Colombia")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech, Sinovac")


def enrich_source(ds: pd.Series, source: str) -> pd.Series:
    return enrich_data(ds, "source_url", source)


def pipeline(ds: pd.Series, source: str) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source, source)
    )


def main(paths):
    source = (
        "https://app.powerbi.com/view?r=eyJrIjoiYjc0NTBhZGMtZGM2NS00YjA0LTljNGYtYTJkNWI1YTJlYzAwIiwid"
        "CI6Ijc0YzBjMjUwLTFjNzctNDA1ZC05YjFlLTlhYzFmNTA4YWJlMyIsImMiOjR9&pageName=ReportSectionad9662980220d3261e68"
    )
    data = read(source).pipe(pipeline, source)
    increment(
        paths=paths,
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
