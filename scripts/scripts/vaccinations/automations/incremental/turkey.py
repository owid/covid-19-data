import requests
import pandas as pd
from bs4 import BeautifulSoup
import vaxutils
import datetime
import pytz
import re


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
    return vaxutils.clean_count(total_vaccinations)


def parse_people_fully_vaccinated(soup: BeautifulSoup) -> int:
    people_fully_vaccinated = re.search(r"var asiyapilankisisayisi2Doz = (\d+);", str(soup)).group(1)
    return vaxutils.clean_count(people_fully_vaccinated)


def parse_people_vaccinated(soup: BeautifulSoup) -> int:
    people_vaccinated = re.search(r"var asiyapilankisisayisi1Doz = (\d+);", str(soup)).group(1)
    return vaxutils.clean_count(people_vaccinated)


def format_date(input: pd.Series) -> pd.Series:
    date = str(datetime.datetime.now(pytz.timezone("Asia/Istanbul")).date())
    return vaxutils.enrich_data(input, 'date', date)


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "Turkey")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine', "Pfizer/BioNTech, Sinovac")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'source_url',
                                "https://covid19asi.saglik.gov.tr/")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(format_date)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://covid19asi.saglik.gov.tr/"
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
