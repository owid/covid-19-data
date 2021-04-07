import requests
import pandas as pd
from bs4 import BeautifulSoup
import vaxutils
import re


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source, verify=False).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    for script in soup.find_all("script"):
        if "new Chart" in str(script):
            chart_data = str(script)
            break
    return pd.Series(data={
        "date": parse_date(chart_data),
        "people_vaccinated": parse_people_vaccinated(chart_data),
        "people_fully_vaccinated": parse_people_fully_vaccinated(chart_data),
        "total_vaccinations": parse_total_vaccinations(chart_data),
    })


def parse_date(df: pd.DataFrame) -> str:
    date = re.search(r"Dati aggiornati al (\d{2}/\d{2}/\d{4})", df).group(1)
    return vaxutils.clean_date(date, "%d/%m/%Y")


def parse_people_vaccinated(df: pd.DataFrame) -> int:
    people_vaccinated = re.search(r"([\d,. ]+) [Vv]accinazioni Prima Dose", df).group(1)
    return vaxutils.clean_count(people_vaccinated)


def parse_people_fully_vaccinated(df: pd.DataFrame) -> int:
    people_fully_vaccinated = re.search(r"([\d,. ]+) [Vv]accinazioni Seconda Dose", df).group(1)
    return vaxutils.clean_count(people_fully_vaccinated)


def parse_total_vaccinations(df: pd.DataFrame) -> int:
    total_vaccinations = re.search(r"([\d,. ]+) [Vv]accinazioni Totali", df).group(1)
    return vaxutils.clean_count(total_vaccinations)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "location", "San Marino")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "vaccine", "Pfizer/BioNTech, Sputnik V")


def enrich_source(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "source_url", "https://vaccinocovid.iss.sm/")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main():
    source = "https://vaccinocovid.iss.sm/"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()
