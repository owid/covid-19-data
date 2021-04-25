import requests
import pandas as pd
from vax.utils.incremental import enrich_data, increment, clean_date


def read(source: str) -> pd.Series:
    data = requests.get(source).json()
    return parse_data(data)


def parse_data(data: dict) -> pd.Series:

    date = clean_date(data["updated"], "%Y/%m/%d")

    total_vaccinations = data["progress"]
    
    return pd.Series(data={
        "date": date,
        "total_vaccinations": total_vaccinations,
    })


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Mongolia")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "source_url", "https://ikon.mn/")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main():
    source = "https://ikon.mn/api/json/vaccine"
    data = read(source).pipe(pipeline)
    increment(
        location=str(data["location"]),
        total_vaccinations=int(data["total_vaccinations"]),
        date=str(data["date"]),
        source_url=str(data["source_url"]),
        vaccine=str(data["vaccine"])
    )


if __name__ == "__main__":
    main()
