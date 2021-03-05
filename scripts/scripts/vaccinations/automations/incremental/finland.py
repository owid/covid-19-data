import datetime
import re
import requests

import pandas as pd
import pytz

import vaxutils


def read(source: str) -> pd.Series:
    data = pd.read_csv(source, sep=";")
    return parse_data(data).pipe(vaxutils.enrich_data, "date", get_date())


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


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "location", "Finland")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "vaccine", "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(
        input,
        "source_url",
        "https://sampo.thl.fi/pivot/prod/en/vaccreg/cov19cov/fact_cov19cov?column=measure-533185.533172.433796.533175&row=cov_vac_dose-533174L"
    )


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main():
    source = "https://sampo.thl.fi/pivot/prod/en/vaccreg/cov19cov/fact_cov19cov.csv?row=cov_vac_dose-533174L&column=measure-533185.533172.433796.533175&"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
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
