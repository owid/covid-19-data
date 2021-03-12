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

    total_vaccinations = int(soup.find_all(class_="counter")[0].text)
    people_vaccinated = int(soup.find_all(class_="counter")[1].text)
    people_fully_vaccinated = int(soup.find_all(class_="counter")[2].text)
    assert total_vaccinations >= people_vaccinated
    assert people_vaccinated >= people_fully_vaccinated

    date = soup.find(class_="fuente").text
    date = re.search(r"\d{2}-\d{2}-\d{4}", date).group(0)
    date = vaxutils.clean_date(date, "%d-%m-%Y")

    data = {
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "date": date,
    }
    return pd.Series(data=data)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "location", "Costa Rica")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "vaccine", "Pfizer/BioNTech")


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
    source = "https://www.ccss.sa.cr/web/coronavirus/vacunacion"
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
