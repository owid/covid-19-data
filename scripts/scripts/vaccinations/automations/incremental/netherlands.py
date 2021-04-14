import datetime
import re
import requests

from bs4 import BeautifulSoup
import dateparser
import pandas as pd

import vaxutils


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")

    date = parse_date(soup)
    total_vaccinations, people_vaccinated, people_fully_vaccinated = parse_data(soup)

    return pd.Series({
        "date": date,
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
    })


def parse_date(soup: BeautifulSoup):
    for h2 in soup.find_all("h2"):
        date = re.search(r"\d+\s\w+ 202\d", h2.text).group(0)
        if date:
            date = dateparser.parse(date, languages=["nl"])
            break
    return str(date.date())


def parse_data(soup: BeautifulSoup) -> str:
    df = pd.read_html(str(soup.find("table")), thousands=".")[0]

    people_vaccinated = int(df.loc[df.Doelgroep == "Totaal", "Eerste dosis"].item())
    people_fully_vaccinated = int(df.loc[df.Doelgroep == "Totaal", "Tweede dosis"].item())
    total_vaccinations = people_vaccinated + people_fully_vaccinated

    return total_vaccinations, people_vaccinated, people_fully_vaccinated


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "location", "Netherlands")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "vaccine", "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(input: pd.Series, source: str) -> pd.Series:
    return vaxutils.enrich_data(input, "source_url", source)


def pipeline(input: pd.Series, source: str) -> pd.Series:
    return (
        input
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source, source)
    )


def main():
    source = "https://www.rivm.nl/covid-19-vaccinatie/cijfers-vaccinatieprogramma"
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
