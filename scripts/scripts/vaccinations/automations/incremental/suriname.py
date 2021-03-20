import datetime
import requests

from bs4 import BeautifulSoup
import pandas as pd
import pytz

import vaxutils


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:

    numbers = soup.find_all(class_="elementor-counter-number")

    return pd.Series(data={
        "people_vaccinated": int(numbers[0]["data-to-value"]),
        "people_fully_vaccinated": int(numbers[1]["data-to-value"]),
        "date": set_date()
    })


def set_date() -> str:
    return str(datetime.datetime.now(pytz.timezone("America/Paramaribo")).date() - datetime.timedelta(days=1))


def add_vaccinations(input: pd.Series) -> pd.Series:
    total_vaccinations = input["people_vaccinated"] + input["people_fully_vaccinated"]
    return vaxutils.enrich_data(input, "total_vaccinations", total_vaccinations)


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "location", "Suriname")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "vaccine", "Oxford/AstraZeneca")


def enrich_source(input: pd.Series, source: str) -> pd.Series:
    return vaxutils.enrich_data(input, "source_url", source)


def pipeline(input: pd.Series, source: str) -> pd.Series:
    return (
        input
        .pipe(add_vaccinations)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source, source)
    )


def main():
    source = "https://laatjevaccineren.sr/"
    data = read(source).pipe(pipeline, source)
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
