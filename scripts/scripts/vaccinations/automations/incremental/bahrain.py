import datetime
import re
import requests

from bs4 import BeautifulSoup
import pandas as pd

import vaxutils


def read(source: str) -> pd.Series:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.16; rv:86.0) Gecko/20100101 Firefox/86.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }
    soup = BeautifulSoup(requests.get(source, headers=headers).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:

    people_vaccinated = int(soup.find_all(class_="count")[0]["data-count"])
    people_fully_vaccinated = int(soup.find_all(class_="count")[1]["data-count"])
    assert people_vaccinated >= people_fully_vaccinated
    total_vaccinations = people_vaccinated + people_fully_vaccinated

    date = soup.find(class_="reportdate").text
    date = re.search(r"\d+ \w+ 202\d", date).group(0)
    date = vaxutils.clean_date(date, "%d %b %Y")

    data = {
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "date": date,
    }
    return pd.Series(data=data)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "location", "Bahrain")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(
        ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sputnik V"
    )


def enrich_source(ds: pd.Series, source: str) -> pd.Series:
    return vaxutils.enrich_data(ds, "source_url", source)


def pipeline(ds: pd.Series, source: str) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source, source)
    )


def main():
    source = "https://healthalert.gov.bh/en/"
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
