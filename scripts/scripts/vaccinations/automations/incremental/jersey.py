import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
import vaxutils

date = None


def read(source: str) -> pd.DataFrame:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    table = soup.find(class_="govstyleTable-default")
    global date
    date = re.search(r"Data applies to: Week ending (\d[\w\s]+\d{4})", soup.text).group(1)
    date = str(date)
    date = vaxutils.clean_date(date, "%d %B %Y")
    return pd.read_html(str(table))[0]


def add_totals(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        people_vaccinated=int(input.loc[input[0] == "Total number of doses", 1].values[0]),
        people_fully_vaccinated=int(input.loc[input[0] == "Total number of first dose vaccinations", 1].values[0]),
    ).assign(
        total_vaccinations=int(input.loc[input[0] == "Total number of second dose vaccinations", 1].values[0]),
    )


def format_date(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(date=date)


def enrich_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        location="Jersey",
        vaccine="Oxford/AstraZeneca, Pfizer/BioNTech",
        source_url="https://www.gov.je/Health/Coronavirus/Vaccine/Pages/VaccinationStatistics.aspx",
    )


def pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(add_totals)
            .pipe(format_date)
            .pipe(enrich_columns)
    )


def main():
    source = "https://www.gov.je/Health/Coronavirus/Vaccine/Pages/VaccinationStatistics.aspx"
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
