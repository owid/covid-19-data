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
    data = {"people_vaccinated": parse_people_vaccinated(soup), "people_fully_vaccinated": parse_people_fully_vaccinated(soup)}
    return pd.Series(data=data)


def get_date(soup: BeautifulSoup) -> str:
    return str((datetime.datetime.now(pytz.timezone("Africa/Johannesburg")) - datetime.timedelta(days=1)).date())


def parse_people_vaccinated(soup: BeautifulSoup) -> str:
    return clean_count(
        soup.find(class_="col col-sm-6 col-md-6 bg-ocean").find(class_="big-chiffre").text
    )


def parse_people_fully_vaccinated(soup: BeautifulSoup) -> str:
    return clean_count(
        soup.find(class_="col col-sm-6 col-md-6 couleur1").find(class_="big-chiffre").text
    )


def add_totals(ds: pd.Series) -> pd.Series:
    total_vaccinations = int(ds['people_vaccinated']) + int(ds['people_fully_vaccinated'])
    return enrich_data(ds, 'total_vaccinations', total_vaccinations)


def format_date(ds: pd.Series) -> pd.Series:
    date = str(datetime.datetime.now(pytz.timezone("Pacific/Noumea")).date() - datetime.timedelta(days=1))
    return enrich_data(ds, 'date', date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "New Caledonia")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Pfizer/BioNTech")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "source_url", "https://gouv.nc/vaccination")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(add_totals)
        .pipe(format_date)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main():
    source = "https://gouv.nc/vaccination"
    data = read(source).pipe(pipeline)
    increment(
        location=str(data["location"]),
        total_vaccinations=int(data["total_vaccinations"]),
        people_vaccinated=int(data["people_vaccinated"]),
        people_fully_vaccinated=int(data["people_fully_vaccinated"]),
        date=str(data["date"]),
        source_url=str(data["source_url"]),
        vaccine=str(data["vaccine"])
    )


if __name__ == "__main__":
    main()
