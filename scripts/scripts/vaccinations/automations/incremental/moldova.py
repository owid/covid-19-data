import datetime
import requests
import time

from bs4 import BeautifulSoup
import pandas as pd
import pytz

import vaxutils


def read(source: str) -> pd.Series:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.16; rv:86.0) Gecko/20100101 Firefox/86.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }
    soup = BeautifulSoup(requests.get(source, headers=headers).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:

    total_vaccinations = int(soup.find(id="stats").find_all("span")[0].text)
    people_vaccinated = int(soup.find(id="stats").find_all("span")[1].text)
    assert total_vaccinations >= people_vaccinated

    data = {
        "people_vaccinated": people_vaccinated,
        "total_vaccinations": total_vaccinations,
    }
    return pd.Series(data=data)


def add_totals(ds: pd.Series) -> pd.Series:
    people_fully_vaccinated = ds["total_vaccinations"] - ds["people_vaccinated"]
    return vaxutils.enrich_data(ds, "people_fully_vaccinated", people_fully_vaccinated)


def format_date(ds: pd.Series) -> pd.Series:
    date = str(datetime.datetime.now(pytz.timezone("Europe/Chisinau")).date() - datetime.timedelta(days=1))
    return vaxutils.enrich_data(ds, "date", date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "location", "Moldova")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "vaccine", "Oxford/AstraZeneca")


def enrich_source(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "source_url", "https://vaccinare.gov.md/")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(add_totals)
        .pipe(format_date)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main():
    source = "https://vaccinare.gov.md/"
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
