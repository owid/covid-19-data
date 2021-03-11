import datetime
import re
import requests

from bs4 import BeautifulSoup
import pandas as pd
import pytz

import vaxutils


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    data = {"date": get_date(soup), "total_vaccinations": parse_total_vaccinations(soup)}
    return pd.Series(data=data)


def get_date(soup: BeautifulSoup) -> str:
    return str((datetime.datetime.now(pytz.timezone("Africa/Johannesburg")) - datetime.timedelta(days=1)).date())


def parse_total_vaccinations(soup: BeautifulSoup) -> str:
    return vaxutils.clean_count(
        soup
        .find(class_="counter-box-content", string=re.compile("Vaccines Administered"))
        .parent
        .find(class_="display-counter")["data-value"]
    )


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "location", "South Africa")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "vaccine", "Johnson&Johnson")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "source_url", "https://sacoronavirus.co.za/")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main():
    source = "https://sacoronavirus.co.za/"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
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
