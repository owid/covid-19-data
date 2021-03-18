import re
import requests

from bs4 import BeautifulSoup
import pandas as pd

import vaxutils


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    data = pd.Series({
        "date": parse_date(soup),
        "total_vaccinations": parse_metric(soup, "Total Doses Administered"),
        "people_vaccinated": parse_metric(soup, "Received First Dose"),
        "people_fully_vaccinated": parse_metric(soup, "Completed Full Vaccination Regimen"),
    })
    return data


def parse_date(soup: BeautifulSoup) -> str:
    for h3 in soup.find_all("h3"):
        if "Vaccination Data" in h3.text:
            break
    date = re.search(r"as of (\d+ \w+ \d+)", h3.text).group(1)
    date = vaxutils.clean_date(date, "%d %b %Y")
    return date


def parse_metric(soup: BeautifulSoup, description: str) -> int:
    value = (
        soup.find("strong", string=description)
        .parent.parent.parent.parent
        .find_all("tr")[-1]
        .text
    )
    return vaxutils.clean_count(value)


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "location", "Singapore")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "vaccine", "Moderna, Pfizer/BioNTech")


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
    source = "https://www.moh.gov.sg/covid-19"
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
