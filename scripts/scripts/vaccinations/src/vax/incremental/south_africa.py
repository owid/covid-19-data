import datetime
import re
import requests

from bs4 import BeautifulSoup
import pandas as pd
import pytz

from vax.utils.incremental import enrich_data, increment, clean_count


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    data = {"date": get_date(soup), "total_vaccinations": parse_total_vaccinations(soup)}
    return pd.Series(data=data)


def get_date(soup: BeautifulSoup) -> str:
    return str((datetime.datetime.now(pytz.timezone("Africa/Johannesburg")) - datetime.timedelta(days=1)).date())


def parse_total_vaccinations(soup: BeautifulSoup) -> str:
    return clean_count(
        soup
        .find(class_="counter-box-content", string=re.compile("Vaccines Administered"))
        .parent
        .find(class_="display-counter")["data-value"]
    )


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "South Africa")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Johnson&Johnson")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "source_url", "https://sacoronavirus.co.za/")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main():
    source = "https://sacoronavirus.co.za/"
    data = read(source).pipe(pipeline)
    increment(
        location=str(data["location"]),
        total_vaccinations=int(data["total_vaccinations"]),
        people_vaccinated=int(data["total_vaccinations"]),
        people_fully_vaccinated=int(data["total_vaccinations"]),
        date=str(data["date"]),
        source_url=str(data["source_url"]),
        vaccine=str(data["vaccine"])
    )


if __name__ == "__main__":
    main()
