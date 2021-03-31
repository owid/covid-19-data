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
        "Accept-Language": "fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }
    soup = BeautifulSoup(requests.get(source, headers=headers).content, "html.parser")

    text = soup.find("div", id="data").find("p").text

    date = re.search(r"На сегодня \(([\d\.]{8})\)", text).group(1)
    date = vaxutils.clean_date(date, "%d.%m.%y")

    people_vaccinated = re.search(r"([\d\s]+) чел\. \([\d\.]+% от населения\) - привито хотя бы одним компонентом вакцины", text).group(1)
    people_vaccinated = vaxutils.clean_count(people_vaccinated)

    people_fully_vaccinated = re.search(r"([\d\s]+) чел\. \([\d\.]+% от населения\) - полностью привито", text).group(1)
    people_fully_vaccinated = vaxutils.clean_count(people_fully_vaccinated)

    total_vaccinations = re.search(r"([\d\s]+) шт\. - всего прививок сделано", text).group(1)
    total_vaccinations = vaxutils.clean_count(total_vaccinations)

    return pd.Series({
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "date": date,
    })


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "location", "Russia")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "vaccine", "Sputnik V, EpiVacCorona")


def enrich_source(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "source_url", "https://gogov.ru/articles/covid-v-stats")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main():
    source = "https://gogov.ru/articles/covid-v-stats"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=data["location"],
        total_vaccinations=int(data["total_vaccinations"]),
        people_vaccinated=int(data["people_vaccinated"]),
        people_fully_vaccinated=int(data["people_fully_vaccinated"]),
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()
