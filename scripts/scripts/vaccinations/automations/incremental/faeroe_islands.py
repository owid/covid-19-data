import re
import requests
import dateparser
import vaxutils
import pandas as pd


def read(source: str) -> pd.DataFrame:
    return pd.read_json(requests.get(source).content)


def add_totals(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        people_vaccinated=int(input["stats"][0]["first_vaccine_number"]),
        people_fully_vaccinated=int(input["stats"][0]["second_vaccine_number"]),
    ).assign(
        total_vaccinations=lambda df: df.people_vaccinated + df.people_fully_vaccinated,
    )


def format_date(input: pd.DataFrame) -> pd.DataFrame:
    date = input["stats"][0]["vaccine_last_update"]
    date = re.search(r"\d+\. \w+", date).group(0)
    date = str(dateparser.parse(date, languages=["da"]).date())
    date = vaxutils.clean_date(date, "%Y-%m-%d")
    return input.assign(date=date)


def enrich_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        location="Faeroe Islands",
        vaccine="Pfizer/BioNTech",
        source_url="https://corona.fo/api",
    )


def pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(add_totals)
            .pipe(format_date)
            .pipe(enrich_columns)
    )


def main():
    source = "https://corona.fo/json/stats"
    data = read(source).pipe(pipeline)

    vaxutils.increment(
        location=str(data['location'].values[0]),
        total_vaccinations=int(data['total_vaccinations'].values[0]),
        people_vaccinated=int(data['people_vaccinated'].values[0]),
        people_fully_vaccinated=int(data['people_fully_vaccinated'].values[0]),
        date=str(data['date'].values[0]),
        source_url=str(data['source_url'].values[0]),
        vaccine=str(data['vaccine'].values[0])
    )


if __name__ == "__main__":
    main()
