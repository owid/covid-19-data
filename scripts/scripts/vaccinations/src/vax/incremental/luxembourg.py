import os
import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd
import tabula

from vax.utils.incremental import enrich_data, increment, clean_date


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    pdf_path = soup.find("a", class_="btn-primary")["href"]  # Get path to newest pdf
    dfs_from_pdf = tabula.read_pdf(pdf_path, pages="all")
    df = pd.DataFrame(dfs_from_pdf[2])  # Hardcoded table location
    col_name = "Unnamed: 2"  # "Total"
    values = sorted(pd.to_numeric(df[col_name].str.replace(r"[^\d]", "", regex=True)).dropna().astype(int))
    assert len(values) == 3
    keys = ("date", "people_fully_vaccinated", "people_vaccinated", "total_vaccinations", "source_url")
    values = (parse_date(df), *values, pdf_path)
    data = dict(zip(keys, values))
    return pd.Series(data=data)


def parse_date(df: dict) -> str:
    # Old
    colnames = df.loc[0]
    date = df.loc[0, "Unnamed: 1"].replace("Journée du ", "")
    # New
    # colnames = df.columns
    # date = df.columns.str.replace("Journée du ", "").values[0]
    _ = [re.search(r"Journée du (\d{1,2}.\d{1,2}.\d{4})", col) for col in colnames.astype(str)]
    col = [col for col in _ if col is not None]
    if len(col) != 1:
        raise ValueError("Something changed in the columns!")
    date = datetime.strptime(col[0].group(1), "%d.%m.%Y").strftime("%Y-%m-%d")
    return date


def parse_total_vaccinations(values: dict) -> int:
    total_vaccinations = int(values[2])
    return total_vaccinations


def parse_people_fully_vaccinated(values: dict) -> int:
    people_fully_vaccinated = int(values[0])
    return people_fully_vaccinated


def parse_people_vaccinated(values: dict) -> int:
    people_vaccinated = int(values[1])
    return people_vaccinated


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'location', "Luxembourg")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'vaccine', "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
    )


def main(paths):
    source = "https://data.public.lu/fr/datasets/covid-19-rapports-journaliers/#_"
    data = read(source).pipe(pipeline)
    increment(
        paths=paths,
        location=data['location'],
        total_vaccinations=data['total_vaccinations'],
        people_vaccinated=data['people_vaccinated'],
        people_fully_vaccinated=data['people_fully_vaccinated'],
        date=data['date'],
        source_url=data['source_url'],
        vaccine=data['vaccine']
    )


if __name__ == "__main__":
    main()
