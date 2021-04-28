import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from vax.utils.incremental import enrich_data, increment, clean_date, clean_count


def read(source: str) -> pd.DataFrame:
    return parse_data(source)


def parse_data(source: str) -> pd.DataFrame:
    df = pd.read_json(url)[["StatisticsDate", "VaccinationStatus", "TotalCount"]]
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


def add_totals(ds: pd.DataFrame) -> pd.DataFrame:
    return df.assign(total_vaccinations=ds['people_vaccinated'] + ds['people_fully_vaccinated'])


def enrich_location(ds: pd.DataFrame) -> pd.DataFrame:
    return df.assign(location="Estonia")


def enrich_vaccine(ds: pd.DataFrame) -> pd.DataFrame:
    return df.assign(ds, 'vaccine', "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_vaccine_name(df: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine_name(date: str) -> str:
        if date < "2021-02-09":
            return "Pfizer/BioNTech"
        return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
    return df.assign(vaccine=df.date.apply(_enrich_vaccine_name))


def enrich_source(ds: pd.DataFrame) -> pd.DataFrame:
    return df.assign(source_url="https://opendata.digilugu.ee")


def pipeline(ds: pd.DataFrame) -> pd.DataFrame:
    return (
        ds
        #.pipe(add_totals)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main():
    source = "https://opendata.digilugu.ee/opendata_covid19_vaccination_total.json"
    destination = "output/Estonia.csv"
    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
