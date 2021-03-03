import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import vaxutils


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    for h2 in soup.find_all("h2", class_="views-field-title"):
        if "COVID-19 blogi" in h2.text:
            url = "https://www.terviseamet.ee" + h2.find("a")["href"]
            break

    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    return pd.Series({
        "date": parse_date(soup),
        "people_vaccinated": parse_people_vaccinated(soup),
        "people_fully_vaccinated": parse_people_fully_vaccinated(soup),
        "source_url": url,
    })


def parse_date(soup: BeautifulSoup) -> str:
    date = soup.find(class_="field-name-post-date").text
    date = vaxutils.clean_date(date, "%d.%m.%Y")
    return date


def parse_people_vaccinated(soup: BeautifulSoup) -> int:
    text = soup.find(class_="node-published").text
    people_vaccinated = re.search(
        r"Eestis on COVID-19 vastu vaktsineerimisi tehtud ([\d\s]+) inimesele, kaks doosi on saanud ([\d\s]+) inimest",
        text
    ).group(1)
    return vaxutils.clean_count(people_vaccinated)


def parse_people_fully_vaccinated(soup: BeautifulSoup) -> int:
    text = soup.find(class_="node-published").text
    people_fully_vaccinated = re.search(
        r"Eestis on COVID-19 vastu vaktsineerimisi tehtud ([\d\s]+) inimesele, kaks doosi on saanud ([\d\s]+) inimest",
        text
    ).group(2)
    return vaxutils.clean_count(people_fully_vaccinated)


def add_totals(ds: pd.Series) -> pd.Series:
    total_vaccinations = ds['people_vaccinated'] + ds['people_fully_vaccinated']
    return vaxutils.enrich_data(ds, 'total_vaccinations', total_vaccinations)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'location', "Estonia")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'vaccine', "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds.pipe(add_totals)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)

    )


def main():
    source = "https://www.terviseamet.ee/et/uudised"
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
