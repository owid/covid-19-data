import os
import datetime

import pandas as pd
import pytz

from vax.utils.incremental import enrich_data, increment


def read(source: str) -> pd.Series:
    data = pd.read_csv(source, sep=";")
    return parse_data(data).pipe(enrich_data, "date", get_date())


def parse_data(df: pd.DataFrame) -> pd.Series:

    people_vaccinated = df.loc[
        (df.Measure == "Administered doses") & (df["Vaccination dose"] == "First dose"), "val"
    ].item()

    people_fully_vaccinated = df.loc[
        (df.Measure == "Administered doses") & (df["Vaccination dose"] == "Second dose"), "val"
    ].item()

    return pd.Series({
        "people_vaccinated": int(people_vaccinated),
        "people_fully_vaccinated": int(people_fully_vaccinated),
        "total_vaccinations": int(people_vaccinated + people_fully_vaccinated),
    })


def get_date() -> str:
    return str((datetime.datetime.now(pytz.timezone("Europe/Helsinki")) - datetime.timedelta(days=1)).date())


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Finland")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(
        ds,
        "source_url",
        (
            "https://sampo.thl.fi/pivot/prod/en/vaccreg/cov19cov/fact_cov19cov?column=measure-533185.533172.433796."
            "533175&row=cov_vac_dose-533174L"
        )
    )


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main(paths):
    source = (
        "https://sampo.thl.fi/pivot/prod/en/vaccreg/cov19cov/fact_cov19cov.csv?row=cov_vac_dose-533174L&"
        "column=measure-533185.533172.433796.533175&"
    )
    data = read(source).pipe(pipeline)
    increment(
        paths=paths,
        location=data["location"],
        total_vaccinations=int(data["total_vaccinations"]),
        people_vaccinated=int(data["people_vaccinated"]),
        people_fully_vaccinated=int(data["people_fully_vaccinated"]),
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()
