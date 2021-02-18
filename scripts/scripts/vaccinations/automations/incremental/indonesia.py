import dateparser
import requests
import pandas as pd
from bs4 import BeautifulSoup
import vaxutils


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    keys = ("date", "people_vaccinated", "people_fully_vaccinated")
    values = (parse_date(soup), parse_people_vaccinated(soup), parse_people_fully_vaccinated(soup))
    data = dict(zip(keys, values))
    return pd.Series(data=data)


def parse_date(soup: BeautifulSoup) -> str:
    date = soup.find(class_="covid-case-container").find(class_="info-date").text.replace("Kondisi ", "")
    date = str(dateparser.parse(date, languages=["id"]).date())
    return date


def parse_people_fully_vaccinated(soup: BeautifulSoup) -> int:
    people_fully_vaccinated = soup.find(class_="description", text="Vaksinasi-2").parent.find(class_="case").text
    return vaxutils.clean_count(people_fully_vaccinated)


def parse_people_vaccinated(soup: BeautifulSoup) -> int:
    people_vaccinated = soup.find(class_="description", text="Vaksinasi-1").parent.find(class_="case").text
    return vaxutils.clean_count(people_vaccinated)


def add_totals(input: pd.Series) -> pd.Series:
    total_vaccinations = input['people_vaccinated'] + input['people_fully_vaccinated']
    return vaxutils.enrich_data(input, 'total_vaccinations', total_vaccinations)


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "Indonesia")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine', "Sinovac")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'source_url',
                                "https://www.kemkes.go.id/")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(add_totals)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://www.kemkes.go.id/"
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
