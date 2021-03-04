import datetime
import re
import requests

from bs4 import BeautifulSoup
import dateparser
import pandas as pd
import tabula

import vaxutils


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")

    pdf_link = soup.find(class_="list-group-item")
    
    date = parse_date(pdf_link)
    total_vaccinations, people_vaccinated, people_fully_vaccinated = parse_data(pdf_link)

    return pd.Series({
        "date": date,
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
    })


def parse_date(pdf_link):
    date = re.search(r"\((\d.*202\d)\)", pdf_link.text).group(1)
    date = dateparser.parse(date, languages=["nl"])
    date = date - datetime.timedelta(days=2) # Delay between vaccination and weekly report
    return str(date.date())


def parse_data(pdf_link) -> str:
    url = "https://www.rivm.nl" + pdf_link["href"]
    kwargs = {"pandas_options": {"dtype": str}}
    dfs_from_pdf = tabula.read_pdf(url, pages="all", **kwargs)

    for df in dfs_from_pdf:
        if "Eerste" in df.columns:
            break

    people_vaccinated = vaxutils.clean_count(df.loc[df.Doelgroep == "Totaal", "Eerste"].item())
    people_fully_vaccinated = vaxutils.clean_count(df.loc[df.Doelgroep == "Totaal", "Tweede"].item())
    total_vaccinations = people_vaccinated + people_fully_vaccinated

    return total_vaccinations, people_vaccinated, people_fully_vaccinated


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "location", "Netherlands")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "vaccine", "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(input: pd.Series, source: str) -> pd.Series:
    return vaxutils.enrich_data(input, "source_url", source)


def pipeline(input: pd.Series, source: str) -> pd.Series:
    return (
        input
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source, source)
    )


def main():
    source = "https://www.rivm.nl/covid-19-vaccinatie/wekelijkse-update-deelname-covid-19-vaccinatie-in-nederland"
    data = read(source).pipe(pipeline, source)
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
