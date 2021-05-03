import requests
import pandas as pd

from vax.utils.files import load_query, load_data
from vax.utils.utils import date_formatter


def read(source: str) -> pd.DataFrame:
    params = load_query("trinidad-and-tobago-metrics", to_str=False)
    data = requests.get(source, params=params).json()
    return parse_data(data)


def parse_data(data: dict) -> int:
    records = [
        {
            "date": x["attributes"]["report_date_str"],
            "people_vaccinated": x["attributes"]["total_vaccinated"],
            "people_fully_vaccinated": x["attributes"]["total_second_dose"],
        }
        for x in data["features"]
    ]
    return pd.DataFrame.from_records(records)


def process_nans(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(people_fully_vaccinated=df.people_fully_vaccinated.fillna(0))


def add_totals(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(total_vaccinations=df.people_vaccinated + df.people_fully_vaccinated)


def format_date(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(date=date_formatter(df.date, "%d/%m/%Y", "%Y-%m-%d"))


def enrich_location(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(location="Trinidad and Tobago")


def enrich_vaccine_name(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(vaccine="Oxford/AstraZeneca")


def enrich_source(df: pd.DataFrame, source: str) -> pd.DataFrame:
    return df.assign(source_url=source)


def merge_legacy(df: pd.DataFrame) -> pd.DataFrame:
    df_legacy = load_data("trinidad-and-tobago-legacy")
    df_legacy = df_legacy[~df_legacy.date.isin(df.date)]
    return pd.concat([df, df_legacy]).sort_values("date")


def pipeline(df: pd.DataFrame, source: str) -> pd.DataFrame:
    return (
        df
        .pipe(process_nans)
        .pipe(add_totals)
        .pipe(enrich_location)
        .pipe(enrich_vaccine_name)
        .pipe(enrich_source, source)
        .pipe(format_date)
        .pipe(merge_legacy)
    )


def main():
    source_ref = "https://experience.arcgis.com/experience/59226cacd2b441c7a939dca13f832112/"
    source = (
        "https://services3.arcgis.com/x3I4DqUw3b3MfTwQ/arcgis/rest/services/service_7a519502598f492a9094fd0ad503cf80/"
        "FeatureServer/0/query"
    )
    destination = "output/Trinidad and Tobago.csv"
    read(source).pipe(pipeline, source_ref).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
