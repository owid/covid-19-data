import datetime
import re

import pytz

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
        time.sleep(5)

        arr = []
        for elem in driver.find_elements_by_class_name("repart-stlucia"):
            arr.append(elem.text)

        for elem in driver.find_elements_by_class_name("h2-blue"):
            date = re.search(r"\w+ \d+, 202\d", elem.text).group(0)
            date = vaxutils.clean_date(date, "%B %d, %Y")

    people_vaccinated = arr[0]
    data = {'people_vaccinated': vaxutils.clean_count(people_vaccinated),
            'date': date}
    return pd.Series(data=data)


def add_totals(ds: pd.Series) -> pd.Series:
    total_vaccinations = ds['people_vaccinated']
    return vaxutils.enrich_data(ds, 'total_vaccinations', total_vaccinations)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'location', "Saint Lucia")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'vaccine', "Oxford/AstraZeneca")


def enrich_source(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'source_url', "https://www.covid19response.lc/")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds.pipe(add_totals)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://www.covid19response.lc/"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=data['location'],
        total_vaccinations=data['total_vaccinations'],
        people_vaccinated=data['people_vaccinated'],
        date=data['date'],
        source_url=data['source_url'],
        vaccine=data['vaccine']
    )


if __name__ == "__main__":
    main()
