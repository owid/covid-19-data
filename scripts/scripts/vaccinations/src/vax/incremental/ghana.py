import os
import re

import pandas as pd

from vax.utils.incremental import enrich_data, increment, clean_count
from vax.utils.utils import get_soup


def read(source: str) -> pd.Series:

    soup = get_soup(source)

    total_vaccinations = clean_count(soup.find(class_="stats-decoration-title").text)
    people_vaccinated = total_vaccinations
    people_fully_vaccinated = 0

    date = re.search(r"\d+ \w+ 202\d", soup.find(class_="stats-decoration-text").text).group(0)
    date = str(pd.to_datetime(date).date())

    return pd.Series(data={
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "date": date,
    })


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Ghana")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca")


def enrich_source(ds: pd.Series, source: str) -> pd.Series:
    return enrich_data(ds, "source_url", source)


def pipeline(ds: pd.Series, source: str) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source, source)
    )


def main(paths):
    source = "https://www.ghanahealthservice.org/covid19/press-releases.php"
    data = read(source).pipe(pipeline, source)
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
