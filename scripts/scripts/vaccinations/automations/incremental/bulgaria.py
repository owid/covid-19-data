import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
import vaxutils
import datetime
import pytz


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    table = soup.find("p", string=re.compile("Поставени ваксини по")).parent.find("table")
    data = pd.read_html(str(table))[0]
    data = data.droplevel(level=0, axis=1)
    data = data[data["Област"] == "Общо"]
    return data.set_index(data.columns[0]).T.squeeze()


def enrich_date(input: pd.Series) -> pd.Series:
    date = str(datetime.datetime.now(pytz.timezone("Europe/Sofia")).date() - datetime.timedelta(days=1))
    return vaxutils.enrich_data(input, 'date', date)


def translate_index(input: pd.Series) -> pd.Series:
    return input.rename({
        'Общо ваксинирани лицас втора доза': 'people_fully_vaccinated',
        'Общо поставени дози': 'total_vaccinations',
    })


def add_totals(input: pd.Series) -> pd.Series:
    people_vaccinated = int(input['total_vaccinations']) - int(input['people_fully_vaccinated'])
    return vaxutils.enrich_data(input, 'people_vaccinated', people_vaccinated)


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "Bulgaria")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine', "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'source_url',
                                "https://coronavirus.bg/bg/statistika")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(translate_index)
            .pipe(add_totals)
            .pipe(enrich_date)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://coronavirus.bg/bg/statistika"
    data = read(source).pipe(pipeline)

    vaxutils.increment(
        location=data['location'],
        total_vaccinations=int(data['total_vaccinations']),
        people_vaccinated=int(data['people_vaccinated']),
        people_fully_vaccinated=int(data['people_fully_vaccinated']),
        date=data['date'],
        source_url=data['source_url'],
        vaccine=data['vaccine']
    )


if __name__ == "__main__":
    main()
