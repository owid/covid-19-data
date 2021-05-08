import os
import re
from datetime import datetime

from bs4 import BeautifulSoup
import pandas as pd

from vax.utils.utils import get_soup
from vax.utils.incremental import clean_count, enrich_data, increment


def read(source: str) -> pd.Series:
    soup = get_soup(source)
    total_vaccinations, people_fully_vaccinated = parse_data(soup)
    return pd.Series({
        "total_vaccinations": total_vaccinations,
        "people_fully_vaccinated": people_fully_vaccinated,
        "source_url": source,
        "date": parse_date(soup)
    })


def parse_data(soup: BeautifulSoup):
    regex = r"Укупно вакцинација: ([\d.]+), од тога ревакцинација: ([\d.]+)"
    matches = re.search(regex, soup.text)

    total_vaccinations = clean_count(matches.group(1))
    people_fully_vaccinated = clean_count(matches.group(2))
    return total_vaccinations, people_fully_vaccinated


def parse_date(soup: BeautifulSoup) -> str:
    regex = r"ажурирано .*"
    elems = soup.find_all("p")
    x = []
    for elem in elems:
        if elem.find(text=re.compile(regex)):
            x.append(elem)
    if len(x) > 1:
        raise ValueError("Format of source has changed")
    date_str = datetime.strptime(x[0].text, "ажурирано %d.%m.%Y").strftime("%Y-%m-%d")
    return date_str


def add_totals(ds: pd.Series) -> pd.Series:
    people_vaccinated = ds.total_vaccinations - ds.people_fully_vaccinated
    return enrich_data(ds, "people_vaccinated", people_vaccinated)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Serbia")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sputnik V")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_vaccine)
        .pipe(enrich_location)
        .pipe(add_totals)
    )


def main(paths):
    source = "https://vakcinacija.gov.rs/"
    data = read(source).pipe(pipeline)
    increment(
        paths=paths,
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
