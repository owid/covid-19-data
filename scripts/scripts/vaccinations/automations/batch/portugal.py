import requests

from bs4 import BeautifulSoup
import pandas as pd


def read(source: str) -> pd.DataFrame:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    source = soup.find(class_="wp-block-button__link")["href"]
    return pd.read_csv(
        source,
        usecols=["DATE", "TYPE", "CUMUL", "CUMUL_VAC_1", "CUMUL_VAC_2"],
        sep=";",
    )


def filter_rows(input: pd.DataFrame) -> pd.DataFrame:
    return input[input.TYPE == "GENERAL"].drop(columns="TYPE")


def rename_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.rename(
        columns={
            "DATE": "date",
            "CUMUL": "total_vaccinations",
            "CUMUL_VAC_1": "people_vaccinated",
            "CUMUL_VAC_2": "people_fully_vaccinated",
        }
    )


def format_date(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        date=pd.to_datetime(input.date, format="%d/%m/%Y").astype(str)
    )


def enrich_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        location="Portugal",
        source_url="https://covid19.min-saude.pt/relatorio-de-vacinacao/",
        vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech",
    )


def pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input
        .pipe(filter_rows)
        .pipe(rename_columns)
        .pipe(format_date)
        .pipe(enrich_columns)
        .sort_values("date")
    )


def main():
    source = "https://covid19.min-saude.pt/relatorio-de-vacinacao/"
    destination = "automations/output/Portugal.csv"
    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
