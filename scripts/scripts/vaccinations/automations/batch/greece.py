import json
from typing import Dict

import requests
import pandas as pd


def get_access_token() -> str:
    with open("vax_dataset_config.json", "rb") as file:
        config = json.load(file)
        return config["greece_api_token"]


def read(source: str) -> pd.DataFrame:
    access_token = get_access_token()
    response = requests.get(source, headers={"Authorization": f"Token {access_token}"})
    return pd.DataFrame.from_records(response.json())


def format_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.rename(
        columns={
            "referencedate": "date",
            "totalvaccinations": "total_vaccinations",
            "totaldistinctpersons": "people_vaccinated",
        }
    )


def aggregate(input: pd.DataFrame) -> pd.DataFrame:
    return input.groupby(by="date", as_index=False)[
        ["total_vaccinations", "people_vaccinated"]
    ].sum()


def enrich_people_fully_vaccinated(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        people_fully_vaccinated=input.total_vaccinations - input.people_vaccinated
    )


def replace_nulls_with_nans(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(people_fully_vaccinated=input.people_fully_vaccinated.replace(0, pd.NA))


def format_date(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(date=input.date.str.slice(0, 10))


def enrich_metadata(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        location="Greece",
        vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech",
        source_url="https://www.data.gov.gr/datasets/mdg_emvolio/",
    )


def pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(format_columns)
        .pipe(aggregate)
        .pipe(enrich_people_fully_vaccinated)
        .pipe(replace_nulls_with_nans)
        .pipe(format_date)
        .pipe(enrich_metadata)
    )


def main():
    source = "https://data.gov.gr/api/v1/query/mdg_emvolio"
    destination = "automations/output/Greece.csv"

    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
