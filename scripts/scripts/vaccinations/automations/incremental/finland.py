import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
import vaxutils


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source, headers={'User-Agent': 'Mozilla/5.0'}).content, "html.parser")
    return parse_data(soup).pipe(vaxutils.enrich_data, 'date', parse_date(soup))


def parse_data(soup: BeautifulSoup) -> pd.Series:
    table = soup.find("table")
    data = pd.read_html(str(table))[0]
    data = data[data["Sairaanhoitopiiri"] == "Kaikki"]
    return data.set_index(data.columns[0]).T.squeeze()


def parse_date(soup: BeautifulSoup) -> str:
    date = soup.find(class_="date").text
    date = re.search(r"[\d-]{10}", date).group(0)
    return date


def translate_index(input: pd.Series) -> pd.Series:
    return input.rename({
        'Ensimmäisen annoksen saaneet': 'people_vaccinated',
        'Toisen annoksen saaneet': 'people_fully_vaccinated',
        'Annetut annokset yhteensä': 'total_vaccinations',
    })


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "Finland")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine', "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'source_url',
                                "https://www.thl.fi/episeuranta/rokotukset/koronarokotusten_edistyminen.html")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(translate_index)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://www.thl.fi/episeuranta/rokotukset/koronarokotusten_edistyminen.html"
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
