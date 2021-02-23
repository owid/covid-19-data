import datetime
import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup
import vaxutils


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    data = {'total_vaccinations': parse_total_vaccinations(soup)}
    return pd.Series(data=data)


def format_date(ds: pd.Series) -> pd.Series:
    local_time = datetime.datetime.now(pytz.timezone("Australia/Sydney"))
    if local_time.hour < 8:
        local_time = local_time - datetime.timedelta(days=1)
    date = str(local_time.date())
    return vaxutils.enrich_data(ds, 'date', date)


def parse_total_vaccinations(soup: BeautifulSoup) -> str:
    total_vaccinations = soup.find(class_="VACCINATIONS").text
    return vaxutils.clean_count(total_vaccinations)


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "Australia")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine', "Pfizer/BioNTech")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'source_url', "https://covidlive.com.au/vaccinations")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(format_date)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://covidlive.com.au/vaccinations"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=str(data['location']),
        total_vaccinations=int(data['total_vaccinations']),
        date=str(data['date']),
        source_url=str(data['source_url']),
        vaccine=str(data['vaccine'])
    )


if __name__ == "__main__":
    main()
