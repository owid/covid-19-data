import datetime
import pytz
from bs4 import BeautifulSoup
import vaxutils
import pandas as pd
import requests



def read(source: str) -> pd.Series:

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.16; rv:86.0) Gecko/20100101 Firefox/86.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }
    soup = BeautifulSoup(requests.get(source, headers=headers).content, 'html.parser')

    div = soup.find(id='m-table')
    table = pd.read_html(str(div))[0]
    table = table.fillna(0)
    table = table.rename(columns={
        'привито, чел.': 'people_vaccinated',
        'привито двумя комп., чел.': 'people_fully_vaccinated'
    })
    return pd.Series({
        "people_vaccinated": parse_people_vaccinated(table),
        "people_fully_vaccinated": parse_people_fully_vaccinated(table),
    })


def parse_people_vaccinated(df: pd.DataFrame) -> int:
    df['people_vaccinated'] = df['people_vaccinated'].str.replace(' ', '')
    df['people_vaccinated'] = df['people_vaccinated'].apply(pd.to_numeric)
    return df['people_vaccinated'].sum()


def parse_people_fully_vaccinated(df: pd.DataFrame) -> int:
    df['people_fully_vaccinated'] = df['people_fully_vaccinated'].str.replace(' ', '')
    df['people_fully_vaccinated'] = df['people_fully_vaccinated'].apply(pd.to_numeric)
    return df['people_fully_vaccinated'].sum()


def format_date(ds: pd.Series) -> pd.Series:
    date = str(datetime.datetime.now(pytz.timezone("Europe/Moscow")).date())
    return vaxutils.enrich_data(ds, 'date', date)


def add_totals(ds: pd.Series) -> pd.Series:
    total_vaccinations = int(ds['people_vaccinated']) + int(ds['people_fully_vaccinated'])
    return vaxutils.enrich_data(ds, 'total_vaccinations', total_vaccinations)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'location', "Russia")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'vaccine', "Sputnik V, EpiVacCorona")


def enrich_source(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'source_url', "https://gogov.ru/articles/covid-v-stats")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds.pipe(add_totals)
            .pipe(format_date)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://gogov.ru/articles/covid-v-stats"
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
