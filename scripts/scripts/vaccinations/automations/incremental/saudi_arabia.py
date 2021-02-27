import datetime
import pytz
import requests
import pandas as pd
import vaxutils
from bs4 import BeautifulSoup


def read(source: str) -> pd.Series:
    widget_id = "2c37ab9e-96ed-4092-8e22-c33da3042d8c"
    data = requests.get(source).json()

    for widget in data["widgets"]:
        if widget["id"] == widget_id:
            break

    soup = BeautifulSoup(widget["text"], "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    return pd.Series({
        "total_vaccinations": parse_total_vaccinations(soup),
    })


def parse_total_vaccinations(soup: BeautifulSoup) -> int:
    total_vaccinations = soup.find("strong").text
    return vaxutils.clean_count(total_vaccinations)


def parse_people_vaccinated(df: pd.DataFrame) -> int:
    return int(df.Vacunados.sum())


def format_date(ds: pd.Series) -> pd.Series:
    date = str(datetime.datetime.now(pytz.timezone("Asia/Riyadh")).date())
    return vaxutils.enrich_data(ds, 'date', date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'location', "Saudi Arabia")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'vaccine', "Pfizer/BioNTech")


def enrich_source(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'source_url', "https://covid19.moh.gov.sa/")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds.pipe(format_date)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://saudimoh.maps.arcgis.com/sharing/rest/content/items/26691a58d8e74c1aa6d5de967caa937f/data?f=json"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=data['location'],
        total_vaccinations=data['total_vaccinations'],
        date=data['date'],
        source_url=data['source_url'],
        vaccine=data['vaccine']
    )


if __name__ == "__main__":
    main()
