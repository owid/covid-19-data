import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
import vaxutils


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    date = parse_date(soup)
    return parse_data(soup).pipe(vaxutils.enrich_data, 'date', date)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    table = soup.find(class_="govstyleTable-default")
    data = pd.read_html(str(table))[0]
    return data.set_index(data.columns[0]).T.squeeze()


def parse_date(soup: BeautifulSoup) -> str:
    date = re.search(r"Data applies to: Week ending (\d[\w\s]+\d{4})", soup.text).group(1)
    date = str(date)
    return vaxutils.clean_date(date, "%d %B %Y")


def translate_index(input: pd.Series) -> pd.Series:
    return input.rename({
        'Total number of first dose vaccinations': 'people_vaccinated',
        'Total number of second dose vaccinations': 'people_fully_vaccinated',
        'Total number of doses': 'total_vaccinations',
    })


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "Jersey")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine', "Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'source_url',
                                "https://www.gov.je/Health/Coronavirus/Vaccine/Pages/VaccinationStatistics.aspx")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(translate_index)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://www.gov.je/Health/Coronavirus/Vaccine/Pages/VaccinationStatistics.aspx"
    data = read(source).pipe(pipeline)

    vaxutils.increment(
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
