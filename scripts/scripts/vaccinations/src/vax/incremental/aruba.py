from datetime import datetime
import pytz

import requests
import pandas as pd

from vax.utils.incremental import enrich_data, increment


def read(source: str) -> pd.Series:
    data = requests.get(source).json()
    for count in data:
        if count[0] == "2nd Vaccine taken":
            people_fully_vaccinated = count[1]
        if count[0] == "1st Vaccine taken":
            people_vaccinated = count[1]

    return pd.Series({
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated
    })


def add_totals(ds: pd.Series) -> pd.Series:
    total_vaccinations = ds["people_vaccinated"] + ds["people_fully_vaccinated"]
    return enrich_data(ds, "total_vaccinations", total_vaccinations)


def enrich_date(ds: pd.Series) -> pd.Series:
    date_str = datetime.now().astimezone(pytz.timezone("America/Aruba")).strftime("%Y-%m-%d")
    return enrich_data(ds, "date", date_str)


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Pfizer/BioNTech")


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Aruba")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(add_totals)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_date)
    )


def main():
    source = "https://api.dvgapp.org/healthcheck-restful-api/public/population/total"
    data = read(source).pipe(pipeline)
    increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        date=data["date"],
        source_url="https://www.government.aw",
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()
