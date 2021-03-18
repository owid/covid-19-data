import datetime
import re
import requests

from bs4 import BeautifulSoup
import pandas as pd
import pytz

import vaxutils


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    data = parse_data(soup)
    return vaxutils.enrich_data(data, "source_url", source)


def parse_data(soup: BeautifulSoup) -> pd.Series:

    regex = r"The total number of COVID-19 vaccines administered to date is ([\d,]+)\. So far, ([\d,]+) .* have received at least one dose of the Pfizer-BioNTech vaccine, while ([\d,]+) have completed the two-dose course"
    matches = re.search(regex, soup.text)

    total_vaccinations = vaxutils.clean_count(matches.group(1))
    people_vaccinated = vaxutils.clean_count(matches.group(2))
    people_fully_vaccinated = vaxutils.clean_count(matches.group(3))
    assert total_vaccinations >= people_vaccinated
    assert people_vaccinated >= people_fully_vaccinated

    return pd.Series({
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
    })


def set_date(ds: pd.Series) -> pd.Series:
    date = str(datetime.datetime.now(pytz.timezone("America/Cayman")).date() - datetime.timedelta(days=1))
    return vaxutils.enrich_data(ds, "date", date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "location", "Cayman Islands")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "vaccine", "Pfizer/BioNTech")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(set_date)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
    )


def main():
    source = "https://www.exploregov.ky/coronavirus-statistics"
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
