from datetime import datetime, timedelta
from urllib.parse import urlparse
import pandas as pd
from bs4 import BeautifulSoup
import vaxutils


def parse_file_link(soup: BeautifulSoup, source: str) -> str:
    href = soup.find(id="download").find_next("a")["href"]
    link = f"https://{urlparse(source).netloc}/{href}"
    return link


def parse_date(soup: BeautifulSoup) -> str:
    return (datetime.strptime(soup.find(class_="date").text, "%d %B %Y") - timedelta(days=1)).strftime("%Y-%m-%d")


def read(source: str) -> pd.Series:
    # Read source
    soup = vaxutils.get_soup(source)
    link = parse_file_link(soup, source)
    ds = vaxutils.read_xlsx_from_url(link, as_series=True)
    # Add date + source
    date = parse_date(soup)
    ds = vaxutils.enrich_data(ds, 'date', date)
    ds = vaxutils.enrich_data(ds, 'source_url', source)
    return ds


def rename_columns(ds: pd.Series) -> pd.Series:
    return ds.rename({
        "First dose administered": "people_vaccinated",
        "Second dose administered": "people_fully_vaccinated",
    })


def add_totals(ds: pd.Series) -> pd.Series:
    total_vaccinations = ds['people_vaccinated'] + ds['people_fully_vaccinated']
    return vaxutils.enrich_data(ds, 'total_vaccinations', total_vaccinations)


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'vaccine', "Pfizer/BioNTech")


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'location', "New Zealand")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(rename_columns)
        .pipe(add_totals)
        .pipe(enrich_vaccine)
        .pipe(enrich_location)
    )


def main():
    source = (
        "https://www.health.govt.nz/our-work/diseases-and-conditions/covid-19-novel-coronavirus/covid-19-data-and-statistics/covid-19-vaccine-data"
    )
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=data['location'],
        total_vaccinations=data['total_vaccinations'],
        people_vaccinated=data['people_vaccinated'],
        people_fully_vaccinated=data['people_fully_vaccinated'],
        date=data['date'],
        source_url=data['source_url'],
        vaccine=data['vaccine']
    )


if __name__ == "__main__":
    main()