import re
from datetime import datetime
import os

import requests
from bs4 import BeautifulSoup
import pandas as pd

from vax.utils.utils import get_soup
from vax.utils.incremental import clean_count


def parse_date(elem) -> str:
    date = elem.find_parent(class_="card").find(class_="news--item-date").text.strip()
    return datetime.strptime(date, "%Y年%m月%d日 %H:%M").strftime("%Y-%m-%d")


def parse_source_url(elem) -> str:
    return elem.find_parent(class_="card").find("a").get("href")


def parse_vaccinations(elem) -> dict:
    # Get news text
    url = elem.find_parent(class_="card").find("a").get("href")
    soup = get_soup(url)
    text = "\n".join([p.text for p in soup.find("article").find_all("p")])

    # Find metrics
    metrics = dict()
    total_vaccinations = re.search(r"疫苗共有(?P<count>[\d,]*)人次", text)
    people_vaccinated = re.search(r"1劑疫苗共有(?P<count>[\d,]*)人次", text)
    people_fully_vaccinated = re.search(r"2劑疫苗共有(?P<count>[\d,]*)人次", text)
    if total_vaccinations:
        metrics["total_vaccinations"] = clean_count(total_vaccinations.group(1))
    if people_vaccinated:
        metrics["people_vaccinated"] = clean_count(people_vaccinated.group(1))
    if people_fully_vaccinated:
        metrics["people_fully_vaccinated"] = clean_count(people_fully_vaccinated.group(1))
    return metrics


def parse_data(soup: BeautifulSoup) -> pd.Series:
    regex_pattern = r"累計共?(?P<count>[\d,]*)人次完成新冠疫苗接種"
    # Get all h3 elements
    elems = soup.find_all("h3")
    # Get data
    records = []
    for elem in elems:
        if elem.find(text=re.compile(regex_pattern)):
            records.append({
                "date": parse_date(elem),
                "source_url": parse_source_url(elem),
                **parse_vaccinations(elem)
            })
    return records


def read(source: str, last_update: str, num_pages_limit: int = 10):
    records = []
    for page_nr in range(1, num_pages_limit):
        # Get soup
        url = f"{source}/{page_nr}/"
        soup = get_soup(url)
        # Get data (if any)
        records_sub = parse_data(soup)
        if records_sub:
            records.extend(records_sub)
            if any([record["date"] <= last_update for record in records_sub]):
                break
    if len(records) > 0:
        records = [record for record in records if record["date"] >= last_update]
        if len(records) > 0:
            return pd.DataFrame(records)
    return None


def enrich_location(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(location="Macao")


def enrich_vaccine(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(vaccine="Sinopharm/Beijing, Pfizer/BioNTech")


def pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
    )


def merge_with_current_data(df: pd.DataFrame, filepath: str) -> pd.DataFrame:
    # Load current data
    if os.path.isfile(filepath):
        df_current = pd.read_csv(filepath)
        #  Merge
        df_current = df_current[~df_current.date.isin(df.date)]
        df = pd.concat([df, df_current]).sort_values(by="date")
    return df


def main():
    source = "https://www.gov.mo/zh-hant/news/page"
    output_file = "output/Macao.csv"
    last_update = pd.read_csv("output/Macao.csv").date.max()
    df = read(source, last_update)
    if df is not None:
        df = df.pipe(pipeline)
        df = merge_with_current_data(df, output_file)
        df.to_csv(output_file, index=False)


if __name__ == "__main__":
    main()
