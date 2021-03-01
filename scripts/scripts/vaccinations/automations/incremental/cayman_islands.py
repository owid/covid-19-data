import re
import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
import vaxutils
import pytz


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    return pd.Series({
        "total_vaccinations": parse_total_vaccinations(soup),
        "people_fully_vaccinated": parse_people_fully_vaccinated(soup),
    })


def parse_total_vaccinations(soup: BeautifulSoup) -> int:
    total_vaccinations = re.search(r"The total number of COVID-19 vaccines administered to date is ([\d,]+)", soup.text)
    return vaxutils.clean_count(total_vaccinations.group(1))


def parse_people_fully_vaccinated(soup: BeautifulSoup) -> int:
    people_fully_vaccinated = re.search(r"([\d,]+) people having completed the two-dose course so far", soup.text)
    return vaxutils.clean_count(people_fully_vaccinated.group(1))


def enrich_people_vaccinated(ds: pd.Series) -> pd.Series:
    people_vaccinated = ds['total_vaccinations'] - ds['people_fully_vaccinated']
    return vaxutils.enrich_data(ds, 'people_vaccinated', people_vaccinated)


def format_date(ds: pd.Series) -> pd.Series:
    date = str(datetime.datetime.now(pytz.timezone("America/Cayman")).date())
    return vaxutils.enrich_data(ds, 'date', date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'location', "Cayman Islands")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'vaccine', "Pfizer/BioNTech")


def enrich_source(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'source_url',
                                "https://www.exploregov.ky/coronavirus-statistics#vaccine-dashboard")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds.pipe(enrich_people_vaccinated)
            .pipe(format_date)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://www.exploregov.ky/coronavirus-statistics#vaccine-dashboard"
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
