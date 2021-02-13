from typing import Dict

import tabula
import pandas as pd
import requests
from bs4 import BeautifulSoup

from utils.pipeline import enrich_total_vaccinations


TRANSLATIONS = {
    "Vaccinationsdato": "date",
    "Antal personer som har påbegyndt vaccination": "people_vaccinated",
    "Antal personer som er færdigvaccineret": "people_fully_vaccinated",
}


def read_datasets_download_link(source: str) -> str:
    page = requests.get(source).content
    soup = BeautifulSoup(page, "html.parser")
    return soup.find("a", text="Download her").get("href")


def read_vaccination_dataset(source: str) -> pd.DataFrame:
    """This PDF contains 13 datasets.
    We are interested in the one that has vaccination data.
    It’s identified because it has `Vaccinationsdato` timeseries data."""

    datasets = tabula.read_pdf(
        source, pages="all", pandas_options={"dtype": str, "header": None}
    )
    matching = [
        dataset for dataset in datasets if "Vaccinationsdato" in dataset[0].values
    ]
    return matching[0]


def format_header(input: pd.DataFrame) -> pd.DataFrame:
    """The way the table is parsed from the PDF makes it seem like
    the header is three consecutive rows.

    The rest is fine, but we need to format this properly.

    It looks like this:
    | 0 | 'Vaccinationsdato' | 'Antal personer som har' | 'Antal personer som har' |
    | 1 | nan                | 'påbegyndt vaccination'  | 'påbegyndt vaccination'  |
    | 2 | nan                | 'pr. dag'                |  nan                     |
    """
    header_rows = (list(row) for row in input[0:3].values)
    header_items = zip(*header_rows)  # (('Vaccinationsdato', nan, nan), (...))

    header = [" ".join([x for x in h if not pd.isna(x)]) for h in header_items]
    return input[3:].set_axis(header, axis="columns")


def read(source: str) -> pd.DataFrame:
    download_link = read_datasets_download_link(source)
    return (
        read_vaccination_dataset(download_link)
        .pipe(format_header)
        .assign(source_url=download_link)
    )


def translate(input: str, translations: Dict[str, str]) -> pd.DataFrame:
    return input.rename(columns=translations)


def format_date(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(date=pd.to_datetime(input.date, format="%d-%m-%Y"))


def parse_number(number: str) -> int:
    return int(number.replace(".", "").replace("-", "0"))


def parse_number_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        people_vaccinated=input.people_vaccinated.apply(parse_number),
        people_fully_vaccinated=input.people_fully_vaccinated.apply(parse_number),
    )


def format_nulls_as_nans(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        people_fully_vaccinated=input.people_fully_vaccinated.replace(0, pd.NA)
    )


def select_output_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input[
        [
            "date",
            "people_vaccinated",
            "people_fully_vaccinated",
            "total_vaccinations",
            "location",
            "source_url",
            "vaccine",
        ]
    ]


def enrich_location(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(location="Denmark")


def enrich_vaccine(input: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine(date: str) -> str:
        if date >= "2021-01-13":
            return "Moderna, Pfizer/BioNTech"
        return "Pfizer/BioNTech"

    return input.assign(vaccine=input.date.astype(str).apply(_enrich_vaccine))


def pre_process(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(translate, TRANSLATIONS).pipe(format_date).pipe(parse_number_columns)
    )


def enrich(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(enrich_total_vaccinations).pipe(enrich_location).pipe(enrich_vaccine)
    )


def post_process(input: pd.DataFrame) -> pd.DataFrame:
    return input.pipe(format_nulls_as_nans).pipe(select_output_columns)


def pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return input.pipe(pre_process).pipe(enrich).pipe(post_process)


def main():
    source = "https://covid19.ssi.dk/overvagningsdata/vaccinationstilslutning"
    destination = "automations/output/Denmark.csv"

    read(source).pipe(pipeline).to_csv(destination index=False)


if __name__ == "__main__":
    main()
