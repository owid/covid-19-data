import datetime
import re
import time
import pytz

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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


def enrich_location(input: pd.Series) -> pd.Series:
    return enrich_data(input, "location", "Colombia")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return enrich_data(input, "vaccine", "Pfizer/BioNTech, Sinovac")


def enrich_source(input: pd.Series, source: str) -> pd.Series:
    return enrich_data(input, "source_url", source)


def pipeline(input: pd.Series, source: str) -> pd.Series:
    return (
        input
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source, source)
    )


def main():
    source = "https://app.powerbi.com/view?r=eyJrIjoiYjc0NTBhZGMtZGM2NS00YjA0LTljNGYtYTJkNWI1YTJlYzAwIiwidCI6Ijc0YzBjMjUwLTFjNzctNDA1ZC05YjFlLTlhYzFmNTA4YWJlMyIsImMiOjR9&pageName=ReportSectionad9662980220d3261e68"
    data = read(source).pipe(pipeline, source)
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
