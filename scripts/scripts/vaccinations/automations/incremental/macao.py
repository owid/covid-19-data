import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd

import vaxutils


def parse_date(elem) -> str:
    date = elem.find_parent(class_="card").find(class_="news--item-date").text.strip()
    return datetime.strptime(date, "%Y年%m月%d日 %H:%M").strftime("%Y-%m-%d")


def parse_source_url(elem) -> str:
    return elem.find_parent(class_="card").find("a").get("href")


def parse_vaccinations(text: str, regex_pattern: str) -> int:
    return int(re.search(regex_pattern, text).group(1).replace(',', ''))


def parse_data(soup: BeautifulSoup) -> pd.Series:
    regex_pattern = r"累計共?(?P<count>[\d,]*)人次完成新冠疫苗接種"
    # Get all h3 elements
    elems = soup.find_all("h3")
    # Get data
    for elem in elems:
        text = elem.find(text=re.compile(regex_pattern))
        if text:
            return pd.Series({
                "date": parse_date(elem),
                "source_url": parse_source_url(elem),
                "total_vaccinations": parse_vaccinations(text, regex_pattern)
            })
    return None


def read(source: str, num_pages_limit: int = 10):
    # Load page
    for page_nr in range(1, num_pages_limit):
        # Get soup
        url = f"{source}/{page_nr}/"
        soup = vaxutils.get_soup(url)
        # Get data
        ds = parse_data(soup)
        if ds is not None:
            return ds
    raise Exception("No news page with vaccination data was found. Check URLs.")


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "location", "Macao")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "vaccine", "Sinopharm/Beijing, Pfizer/BioNTech")


def add_totals(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "people_vaccinated", ds.total_vaccinations)


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(add_totals)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
    )


def main():
    source = "https://www.gov.mo/zh-hant/news/page"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()
