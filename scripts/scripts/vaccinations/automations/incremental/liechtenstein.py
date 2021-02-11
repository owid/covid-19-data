import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
import vaxutils

date = None


def read(source: str) -> pd.DataFrame:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    table = soup.find(class_="geo-unit-vacc-doses-data__table")
    global date
    date = soup.find(class_="detail-card__source").find("span").text
    date = re.search(r"[\d\.]{10}", date).group(0)
    date = str(date)
    date = vaxutils.clean_date(date, "%d.%m.%Y")
    return pd.read_html(str(table))[0]


def add_totals(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        total_vaccinations=
        input.loc[input["per 100 inhabitants"] == "Administered vaccines", "absolute numbers"].values[0],
    )


def format_date(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(date=date)


def enrich_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        location="Liechtenstein",
        vaccine="Moderna, Pfizer/BioNTech",
        source_url="https://www.covid19.admin.ch/en/epidemiologic/vacc-doses?detGeo=FL",
    )


def pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(add_totals)
            .pipe(format_date)
            .pipe(enrich_columns)
    )


def main():
    source = "https://www.covid19.admin.ch/en/epidemiologic/vacc-doses?detGeo=FL"
    data = read(source).pipe(pipeline)

    vaxutils.increment(
        location=str(data['location'].values[0]),
        total_vaccinations=int(data['total_vaccinations'].values[0]),
        date=str(data['date'].values[0]),
        source_url=str(data['source_url'].values[0]),
        vaccine=str(data['vaccine'].values[0])
    )


if __name__ == "__main__":
    main()
