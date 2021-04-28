import re

from bs4 import BeautifulSoup
import dateparser
import pandas as pd

from vax.utils.incremental import enrich_data, increment, clean_count
from vax.utils.utils import get_soup


def read(source: str) -> pd.Series:
    soup = get_soup(source)
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:

    for p in soup.find_all("p"):

        if "Primera dosis" in p.text:
            people_vaccinated = clean_count(re.search(r"[\d,]{6,}", p.text).group(0))

        elif "Total dosis aplicadas" in p.text:
            total_vaccinations = clean_count(re.search(r"[\d,]{6,}", p.text).group(0))

        elif "Población completamente vacunada" in p.text:
            people_fully_vaccinated = clean_count(re.search(r"[\d,]{6,}", p.text).group(0))

    date = soup.find("h6").text.replace("Acumulados al ", "")
    date = str(dateparser.parse(date, languages=["es"]).date())

    data = {
        "date": date,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "total_vaccinations": total_vaccinations,
    }
    return pd.Series(data=data)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Dominican Republic")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Sinovac")


def enrich_source(ds: pd.Series, source: str) -> pd.Series:
    return enrich_data(ds, "source_url", source)


def pipeline(ds: pd.Series, source: str) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source, source)
    )


def main():
    source = "https://vacunate.gob.do/"
    data = read(source).pipe(pipeline, source)
    increment(
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
