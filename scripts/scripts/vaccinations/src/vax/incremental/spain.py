import datetime
import requests

from bs4 import BeautifulSoup
import pandas as pd
import pytz

from vax.utils.incremental import enrich_data, increment, clean_count


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:

    numbers = soup.find(class_="cifras-coronavirus").find_all(class_="cifra")

    return pd.Series(data={
        "total_vaccinations": clean_count(numbers[1].text),
        "people_fully_vaccinated": clean_count(numbers[2].text),
        "date": set_date()
    })


def set_date() -> str:
    return str(datetime.datetime.now(pytz.timezone("Europe/Madrid")).date() - datetime.timedelta(days=1))


def add_vaccinated(ds: pd.Series) -> pd.Series:
    people_vaccinated = ds["total_vaccinations"] - ds["people_fully_vaccinated"]
    return enrich_data(ds, "people_vaccinated", people_vaccinated)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Spain")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(ds: pd.Series, source: str) -> pd.Series:
    return enrich_data(ds, "source_url", source)


def pipeline(ds: pd.Series, source: str) -> pd.Series:
    return (
        ds
        .pipe(add_vaccinated)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source, source)
    )


def main():
    source = "https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/vacunaCovid19.htm"
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
