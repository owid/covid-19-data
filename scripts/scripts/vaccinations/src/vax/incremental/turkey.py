import os
import datetime
import re

import pytz
import requests
import pandas as pd
from bs4 import BeautifulSoup

from vax.utils.incremental import enrich_data, increment, clean_count


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    keys = ("total_vaccinations", "people_vaccinated", "people_fully_vaccinated")
    values = (parse_total_vaccinations(soup),
              parse_people_vaccinated(soup), parse_people_fully_vaccinated(soup))
    data = dict(zip(keys, values))
    return pd.Series(data=data)


def parse_total_vaccinations(soup: BeautifulSoup) -> int:
    total_vaccinations = re.search(r"var yapilanasisayisi = (\d+);", str(soup)).group(1)
    return clean_count(total_vaccinations)


def parse_people_fully_vaccinated(soup: BeautifulSoup) -> int:
    people_fully_vaccinated = re.search(r"var asiyapilankisisayisi2Doz = (\d+);", str(soup)).group(1)
    return clean_count(people_fully_vaccinated)


def parse_people_vaccinated(soup: BeautifulSoup) -> int:
    people_vaccinated = re.search(r"var asiyapilankisisayisi1Doz = (\d+);", str(soup)).group(1)
    return clean_count(people_vaccinated)


def format_date(ds: pd.Series) -> pd.Series:
    date = str(datetime.datetime.now(pytz.timezone("Asia/Istanbul")).date())
    return enrich_data(ds, 'date', date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'location', "Turkey")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'vaccine', "Pfizer/BioNTech, Sinovac")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'source_url', "https://covid19asi.saglik.gov.tr/")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(format_date)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main(paths):
    source = "https://covid19asi.saglik.gov.tr/"
    data = read(source).pipe(pipeline)
    increment(
        paths=paths,
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
