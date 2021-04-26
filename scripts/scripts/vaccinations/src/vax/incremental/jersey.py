import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from vax.utils.incremental import enrich_data, increment, clean_date


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    date = parse_date(soup)
    return parse_data(soup).pipe(enrich_data, 'date', date)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    table = soup.find(class_="govstyleTable-default")
    data = pd.read_html(str(table))[0]
    return data.set_index(data.columns[0]).T.squeeze()


def parse_date(soup: BeautifulSoup) -> str:
    date = re.search(r"Data applies to: Week ending (\d[\w\s]+\d{4})", soup.text).group(1)
    date = str(date)
    return clean_date(date, "%d %B %Y")


def translate_index(ds: pd.Series) -> pd.Series:
    return ds.rename({
        'Total number of first dose vaccinations': 'people_vaccinated',
        'Total number of second dose vaccinations': 'people_fully_vaccinated',
        'Total number of doses': 'total_vaccinations',
    })


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'location', "Jersey")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'vaccine', "Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(
        ds,
        'source_url',
        "https://www.gov.je/Health/Coronavirus/Vaccine/Pages/VaccinationStatistics.aspx"
    )


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(translate_index)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main():
    source = "https://www.gov.je/Health/Coronavirus/Vaccine/Pages/VaccinationStatistics.aspx"
    data = read(source).pipe(pipeline)

    increment(
        location=str(data['location']),
        total_vaccinations=int(data['total_vaccinations']),
        people_vaccinated=int(data['people_vaccinated']),
        people_fully_vaccinated=int(data['people_fully_vaccinated']),
        date=str(data['date']),
        source_url=str(data['source_url']),
        vaccine=str(data['vaccine'])
    )


if __name__ == "__main__":
    main()
