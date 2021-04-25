import requests
import pandas as pd
from bs4 import BeautifulSoup
from vax.utils.incremental import enrich_data, increment, clean_count
import datetime
import pytz


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    data = {'total_vaccinations': parse_total_vaccinations(soup)}
    return pd.Series(data=data)


def parse_total_vaccinations(soup: BeautifulSoup) -> int:
    total_vaccinations = soup.find(class_="doses").find(class_="counter").text
    return clean_count(total_vaccinations)


def format_date(ds: pd.Series) -> pd.Series:
    date = str(datetime.datetime.now(pytz.timezone("Asia/Dubai")).date() - datetime.timedelta(days=1))
    return enrich_data(ds, 'date', date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'location', "United Arab Emirates")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'vaccine',
                                "Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sinopharm/Wuhan, Sputnik V")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'source_url',
                                "http://covid19.ncema.gov.ae/en")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds.pipe(format_date)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "http://covid19.ncema.gov.ae/en"
    data = read(source).pipe(pipeline)
    increment(
        location=data['location'],
        total_vaccinations=int(data['total_vaccinations']),
        date=data['date'],
        source_url=data['source_url'],
        vaccine=data['vaccine']
    )


if __name__ == "__main__":
    main()
