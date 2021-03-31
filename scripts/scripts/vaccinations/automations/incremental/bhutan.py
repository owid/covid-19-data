import requests
import pandas as pd
from bs4 import BeautifulSoup
import vaxutils
import re


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source, verify=False).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    keys = ("date", "people_vaccinated")
    values = (parse_date(soup.get_text()),
              parse_people_vaccinated(soup.get_text()))
    data = dict(zip(keys, values))
    return pd.Series(data=data)


def parse_date(df: pd.DataFrame) -> str:
    date = re.search(r"\d+ \w+ 202\d", df).group(0)
    return vaxutils.clean_date(date, "%d %B %Y")


def parse_people_vaccinated(df: pd.DataFrame) -> int:
    people_vaccinated = re.search(r"A total of \d+\,?\d+ individuals vaccinated across the country", df).group(0)
    return vaxutils.clean_count(people_vaccinated)


def add_totals(ds: pd.Series) -> pd.Series:
    total_vaccinations = ds['people_vaccinated']
    return vaxutils.enrich_data(ds, 'total_vaccinations', total_vaccinations)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'location', "Bhutan")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'vaccine', "Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'source_url',
                                "http://www.moh.gov.bt/vaccination-status/")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds.pipe(add_totals)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "http://www.moh.gov.bt/vaccination-status/"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=data['location'],
        total_vaccinations=data['total_vaccinations'],
        people_vaccinated=data['people_vaccinated'],
        date=data['date'],
        source_url=data['source_url'],
        vaccine=data['vaccine']
    )


if __name__ == "__main__":
    main()
