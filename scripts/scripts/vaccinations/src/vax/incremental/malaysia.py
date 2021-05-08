import os
from datetime import datetime
import json

import pandas as pd
import requests

from vax.utils.incremental import enrich_data, increment, clean_count


def read(source: str) -> pd.Series:
    json_data = requests.get(source).json()
    return parse_data(json_data)


def parse_data(data: dict) -> pd.Series:
    dose1 = clean_count(data["data"][0]["vakdose1"])
    dose2 = clean_count(data["data"][0]["vakdose2"])
    data = pd.Series({
        "date": datetime.fromtimestamp(data["updated"] // 1000).strftime("%Y-%m-%d"),
        "people_vaccinated": dose1,
        "people_fully_vaccinated": dose2,
        "total_vaccinations": dose1 + dose2
    })
    return data


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Malaysia")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Pfizer/BioNTech, Sinovac")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "source_url", "https://www.vaksincovid.gov.my/statistik/")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main(paths):
    source = "https://covidbucketbbc.s3-ap-southeast-1.amazonaws.com/heatdata.json"
    data = read(source).pipe(pipeline)
    increment(
        paths=paths,
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
