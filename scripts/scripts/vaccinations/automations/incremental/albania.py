import datetime
import re
import requests

from bs4 import BeautifulSoup
import pandas as pd
import pytz

import vaxutils


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    source = soup.find(class_="sidebar").find(class_="text-dark")["href"]

    soup = BeautifulSoup(requests.get(source).content, "html.parser")

    regex = r"që nga fillimi i vaksinimit janë kryer ([\d,]+) vaksinime"
    total_vaccinations = re.search(regex, soup.text).group(1)
    total_vaccinations = vaxutils.clean_count(total_vaccinations)

    return pd.Series({
        "total_vaccinations": total_vaccinations,
        "source_url": source,
    })


def set_date(ds: pd.Series) -> pd.Series:
    date = str(datetime.datetime.now(pytz.timezone("Europe/Tirane")).date() - datetime.timedelta(days=1))
    return vaxutils.enrich_data(ds, "date", date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "location", "Albania")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "vaccine", "Pfizer/BioNTech")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(set_date)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
    )


def main():
    source = "https://coronavirus.al/masa/"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()
