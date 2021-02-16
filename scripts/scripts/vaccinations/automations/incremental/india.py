import re
import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
import vaxutils


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    data = {'date': parse_date(soup), 'total_vaccinations': parse_total_vaccinations(soup)}
    return pd.Series(data=data)


def parse_date(soup: BeautifulSoup) -> str:
    date = soup.find(id="site-dashboard").find("h5").text
    date = re.search(r"\d+\s\w+\s+202\d", date).group(0)
    date = datetime.datetime.strptime(date, "%d %B %Y") - datetime.timedelta(days=1)
    date = str(date.date())
    return date


def parse_total_vaccinations(soup: BeautifulSoup) -> str:
    total_vaccinations = soup.find(class_="coviddata").text
    return vaxutils.clean_count(total_vaccinations)


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "India")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine', "Covaxin, Oxford/AstraZeneca")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'source_url', "https://www.mohfw.gov.in/")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://www.mohfw.gov.in/"
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
