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

        people_vaccinated = \
            re.findall(r'\d+\.?\d+', re.search(r"De los \d+\.?\d+ vacunados, \d+\.?\d+", driver.page_source).group(0))[
                0].replace(".", "")
        people_fully_vaccinated = \
            re.findall(r'\d+\.?\d+', re.search(r"De los \d+\.?\d+ vacunados, \d+\.?\d+", driver.page_source).group(0))[
                1].replace(".", "")

    data = {'people_vaccinated': vaxutils.clean_count(people_vaccinated),
            'people_fully_vaccinated': vaxutils.clean_count(people_fully_vaccinated)}
    return pd.Series(data=data)


def add_totals(ds: pd.Series) -> pd.Series:
    total_vaccinations = ds['people_vaccinated'] + ds['people_fully_vaccinated']
    return vaxutils.enrich_data(ds, 'total_vaccinations', total_vaccinations)


def format_date(ds: pd.Series) -> pd.Series:
    local_time = datetime.datetime.now(pytz.timezone("Africa/Malabo"))
    local_time = local_time - datetime.timedelta(days=1)
    date = str(local_time.date())
    return vaxutils.enrich_data(ds, 'date', date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'location', "Equatorial Guinea")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'vaccine', "Sinopharm/Beijing")


def enrich_source(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'source_url', "https://guineasalud.org/estadisticas/")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds.pipe(add_totals)
            .pipe(format_date)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://guineasalud.org/estadisticas/"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=data['location'],
        total_vaccinations=data['total_vaccinations'],
        people_vaccinated=data['people_vaccinated'],
        people_fully_vaccinated=data['people_fully_vaccinated'],
        date=data['date'],
        source_url=data['source_url'],
        vaccine=data['vaccine']
    )


if __name__ == "__main__":
    main()
