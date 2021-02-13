import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
import vaxutils


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    table = soup.find(class_="govstyleTable-default")
    df = pd.read_html(str(table))[0]
    df.set_index(df.columns[0], inplace=True)
    ds = df.T.squeeze()
    dt = re.search(r"Data applies to: Week ending (\d[\w\s]+\d{4})", soup.text).group(1)
    dt = str(dt)
    dt = vaxutils.clean_date(dt, "%d %B %Y")
    return ds.append(pd.Series([dt], index=['date']))


def enrich_columns(input: pd.Series) -> pd.Series:
    input.rename(
        index={'Total number of first dose vaccinations': 'people_vaccinated',
               'Total number of second dose vaccinations': 'people_fully_vaccinated',
               'Total number of doses': 'total_vaccinations'},
        inplace=True)
    return input


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "Jersey")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine', "Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'source_url',
                                "https://www.gov.je/Health/Coronavirus/Vaccine/Pages/VaccinationStatistics.aspx")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(enrich_columns)
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
