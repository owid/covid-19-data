import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
import vaxutils


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    data = {'date': parse_date(soup), 'total_vaccinations': parse_total_vaccinations(soup)}
    return pd.Series(data=data)


def parse_date(soup: BeautifulSoup) -> str:
    date = soup.find(class_="detail-card__source").text
    date = re.search(r"[\d\.]{10}", date).group(0)
    date = str(date)
    return vaxutils.clean_date(date, "%d.%m.%Y")


def parse_total_vaccinations(soup: BeautifulSoup) -> str:
    table = soup.find(class_="geo-unit-vaccination-data__table")
    total_vaccinations = pd.read_html(str(table))[0].loc[1]['absolute numbers']
    return total_vaccinations


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "Liechtenstein")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine', "Moderna, Pfizer/BioNTech")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'source_url',
                                "https://www.covid19.admin.ch/en/epidemiologic/vacc-doses?detGeo=FL")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://www.covid19.admin.ch/en/epidemiologic/vacc-doses?detGeo=FL"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=str(data['location']),
        total_vaccinations=int(data['total_vaccinations']),
        date=str(data['date']),
        source_url=str(data['source_url']),
        vaccine=str(data['vaccine'])
    )


if __name__ == "__main__":
    main()
