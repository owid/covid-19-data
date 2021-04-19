from datetime import datetime
import pytz

import requests
import pandas as pd

from vax.utils.incremental import enrich_data, increment


def read(source: str) -> pd.Series:
    data = parse_data(source)
    return pd.Series({
        "total_vaccinations": data["total"],
        "people_vaccinated": data["total.dosis1"],
        "people_fully_vaccinated": data["total.dosis2"]
    })


def parse_data(source: str) -> dict:
    data = requests.get(source).json()
    return {d["code"]: d["count"] for d in data["stats"]}


def enrich_date(ds: pd.Series) -> pd.Series:
    date_str = date_str = datetime.now().astimezone(pytz.timezone('America/Curacao')).strftime("%Y-%m-%d")
    return enrich_data(ds, "date", date_str)


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Pfizer/BioNTech, Moderna")


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Curacao")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_date)
    )


def main():
    source = "https://bakuna-counter.ibis-management.com/init/"
    data = read(source).pipe(pipeline)
    increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        date=data["date"],
        source_url="https://bakuna.cw",
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()