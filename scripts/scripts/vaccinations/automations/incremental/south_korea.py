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

    people_vaccinated = vaxutils.clean_count(
        soup
        .find(class_="status_infoArea")
        .find(class_="round1")
        .find(class_="big")
        .text
    )

    people_fully_vaccinated = vaxutils.clean_count(
        soup
        .find(class_="status_infoArea")
        .find(class_="round2")
        .find(class_="big")
        .text
    )

    total_vaccinations = people_vaccinated + people_fully_vaccinated

    date = str((datetime.datetime.now(pytz.timezone("Asia/Seoul")) - datetime.timedelta(days=1)).date())

    data = {
        "date": date,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "total_vaccinations": total_vaccinations,
    }
    return pd.Series(data=data)


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "location", "South Korea")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "source_url", "http://ncv.kdca.go.kr/")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main():
    source = "http://ncv.kdca.go.kr/"
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
