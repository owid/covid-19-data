import datetime
import re
import requests

from bs4 import BeautifulSoup
import dateparser
import pandas as pd

from vax.utils.incremental import enrich_data, increment


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source[0]).content, "html.parser")
    total_soup = BeautifulSoup(requests.get(source[1]).content, "html.parser")
    date = parse_date(soup)
    people_vaccinated, people_fully_vaccinated = parse_data(soup)
    total_vaccinations = parse_total_data(total_soup)

    return pd.Series({
        "date": date,
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
    })


def parse_date(soup: BeautifulSoup):
    for h2 in soup.find_all("h2"):
        date = re.search(r"\d+\s\w+ 202\d", h2.text).group(0)
        if date:
            date = dateparser.parse(date, languages=["nl"])
            break
    return str(date.date())


def parse_data(soup: BeautifulSoup) -> str:
    df = pd.read_html(str(soup.find("table")), thousands=".")[0]

    people_vaccinated = int(df.loc[df.Doelgroep == "Totaal", "Eerste dosis"].item())
    people_fully_vaccinated = int(df.loc[df.Doelgroep == "Totaal", "Tweede dosis"].item())

    return people_vaccinated, people_fully_vaccinated


def parse_total_data(soup: BeautifulSoup) -> str:
    div = soup.find("div", {"class": "sc-bdfBwQ two-kpi-section___StyledBox-wk5ja3-0 jlCFBI djRldS"})
    return div.find("div", {"color": "data.primary"}).get_text()


def enrich_location(input: pd.Series) -> pd.Series:
    return enrich_data(input, "location", "Netherlands")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return enrich_data(input, "vaccine", "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(input: pd.Series, source: str) -> pd.Series:
    return enrich_data(input, "source_url", source)


def pipeline(input: pd.Series, source: str) -> pd.Series:
    return (
        input
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source, source)
    )


def main():
    source = list()
    source += "https://www.rivm.nl/covid-19-vaccinatie/cijfers-vaccinatieprogramma"
    source += "https://coronadashboard.rijksoverheid.nl/landelijk/vaccinaties"
    data = read(source).pipe(pipeline, source)
    increment(
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
