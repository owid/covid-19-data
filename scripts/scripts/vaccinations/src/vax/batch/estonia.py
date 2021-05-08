import os
import re

import requests
from bs4 import BeautifulSoup
import pandas as pd

from vax.utils.incremental import enrich_data, increment, clean_date, clean_count


def read(source: str) -> pd.DataFrame:
    return parse_data(source)


def parse_data(source: str) -> pd.DataFrame:
    df = pd.read_json(source)[["StatisticsDate", "VaccinationStatus", "TotalCount"]]
    df = (
        df
        .pivot(
            index="StatisticsDate",
            columns="VaccinationStatus",
            values="TotalCount"
        )
        .reset_index()
        .rename(columns={
            "Completed": "people_fully_vaccinated",
            "InProgress": "people_vaccinated",
            "StatisticsDate": "date"
        })
    )
    return df


def add_totals(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        total_vaccinations=df.people_fully_vaccinated + df.people_vaccinated 
    )


def enrich_location(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(location="Estonia")


def enrich_vaccine_name(df: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine_name(date: str) -> str:
        if date < "2021-01-14":
            return "Pfizer/BioNTech"
        elif "2021-01-14" <= date < "2021-02-09":
            return "Moderna, Pfizer/BioNTech"
        elif "2021-02-09" <= date: 
            return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
    return df.assign(vaccine=df.date.apply(_enrich_vaccine_name))


def enrich_source(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(source_url="https://opendata.digilugu.ee")


def pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .pipe(add_totals)
        .pipe(enrich_location)
        .pipe(enrich_vaccine_name)
        .pipe(enrich_source)
    )


def main(paths):
    source = "https://opendata.digilugu.ee/opendata_covid19_vaccination_total.json"
    destination = paths.tmp_vax_loc("Estonia")
    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
