import os
import json

import requests
import pandas as pd


def read(source: str, access_token: str) -> pd.DataFrame:
    response = requests.get(source, headers={"Authorization": f"Token {access_token}"})
    return pd.DataFrame.from_records(response.json())


def format_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "referencedate": "date",
            "totalvaccinations": "total_vaccinations",
            "totaldistinctpersons": "people_vaccinated",
        }
    )


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(by="date", as_index=False)[
        ["total_vaccinations", "people_vaccinated"]
    ].sum()


def enrich_people_fully_vaccinated(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        people_fully_vaccinated=df.total_vaccinations - df.people_vaccinated
    )


def replace_nulls_with_nans(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(people_fully_vaccinated=df.people_fully_vaccinated.replace(0, pd.NA))


def format_date(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(date=df.date.str.slice(0, 10))


def enrich_metadata(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        location="Greece",
        vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech",
        source_url="https://www.data.gov.gr/datasets/mdg_emvolio/",
    )


def pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(format_columns)
        .pipe(aggregate)
        .pipe(enrich_people_fully_vaccinated)
        .pipe(replace_nulls_with_nans)
        .pipe(format_date)
        .pipe(enrich_metadata)
    )


def main(paths, access_token: str):
    source = "https://data.gov.gr/api/v1/query/mdg_emvolio"
    destination = paths.out_tmp("Greece")
    read(source, access_token).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
