import re
import requests

from bs4 import BeautifulSoup
import pandas as pd

import vaxutils


def read(source: str) -> pd.Series:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.16; rv:86.0) Gecko/20100101 Firefox/86.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }
    soup = BeautifulSoup(requests.get(source, headers=headers).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:

    data = pd.Series(dtype="int")

    spans = soup.find("table").find_all("span")

    data["people_vaccinated"] = int(re.sub(r"[^\d]", "", spans[-3].text))
    data["people_fully_vaccinated"] = int(re.sub(r"[^\d]", "", spans[-2].text))
    data["total_vaccinations"] = data["people_vaccinated"] + data["people_fully_vaccinated"]

    date = re.search(r"[\d-]{10}", spans[0].text).group(0)
    data["date"] = vaxutils.clean_date(date, "%d-%m-%Y")

    return data


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "location", "Morocco")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "vaccine", "Oxford/AstraZeneca, Sinopharm/Beijing")


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
    source = "http://www.covidmaroc.ma/pages/Accueilfr.aspx"
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
