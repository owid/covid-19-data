import re
from datetime import datetime
import os

import requests
from bs4 import BeautifulSoup
import pandas as pd

import vaxutils


def parse_date(elem) -> str:
    date = elem.find_parent(class_="card").find(class_="news--item-date").text.strip()
    return datetime.strptime(date, "%Y年%m月%d日 %H:%M").strftime("%Y-%m-%d")


def parse_source_url(elem) -> str:
    return elem.find_parent(class_="card").find("a").get("href")


def parse_vaccinations(elem) -> dict:
    # Get news text
    url = elem.find_parent(class_="card").find("a").get("href")
    soup = vaxutils.get_soup(url)
    text = "\n".join([p.text for p in soup.find("article").find_all("p")])

    # Find metrics
    metrics = dict()
    total_vaccinations = re.search(r"疫苗共有(?P<count>[\d,]*)人次", text)
    people_vaccinated = re.search(r"1劑疫苗共有(?P<count>[\d,]*)人次", text)
    people_fully_vaccinated = re.search(r"2劑疫苗共有(?P<count>[\d,]*)人次", text)
    if total_vaccinations:
        metrics["total_vaccinations"] = vaxutils.clean_count(total_vaccinations.group(1))
    if people_vaccinated:
        metrics["people_vaccinated"] = vaxutils.clean_count(people_vaccinated.group(1))
    if people_fully_vaccinated:
        metrics["people_fully_vaccinated"] = vaxutils.clean_count(people_fully_vaccinated.group(1))
    return metrics


def parse_data(soup: BeautifulSoup) -> pd.Series:
    regex_pattern = r"(新冠|疫苗接種|疫苗)"
    # Get all h3 elements
    elems = soup.find_all("h3")
    # Get data
    records = []
    for elem in elems:
        if elem.find(text=re.compile(regex_pattern)):
            date = parse_date(elem)
            # print("added", date)
            records.append({
                "date": date,
                "source_url": parse_source_url(elem),
                **parse_vaccinations(elem)
            })
    return records


def postprocess(df):
    col_ints = ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]
    # 1. remove entire NaN rows
    df = df[~df[col_ints].isnull().all(axis=1)]
    # 2. Combine
    df = df.sort_values(by=["date", "source_url"])
    df = df.fillna(method="ffill")
    df = df.drop_duplicates(subset=["date"], keep="last")
    return df


def read(source: str, last_update: str, num_pages_limit: int = 10):
    records = []
    for page_nr in range(1, num_pages_limit):
        # print(page_nr)
        # Get soup
        url = f"{source}/{page_nr}/"
        soup = vaxutils.get_soup(url)
        # Get data (if any)
        records_sub = parse_data(soup)
        if records_sub:
            records.extend(records_sub)
            if any([record["date"] <= last_update for record in records_sub]):
                # print("Dates exceding!  ", str([record["date"] for record in records_sub]))
                break
    if len(records) > 0:
        records = [record for record in records if record["date"] >= last_update]
        if len(records) > 0:
            return postprocess(pd.DataFrame(records))
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
    col_ints = ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]
    # Load current data
    if os.path.isfile(filepath):
        df_current = pd.read_csv(filepath)
        #  Merge
        df_current = df_current[~df_current.date.isin(df.date)]
        df = pd.concat([df, df_current]).sort_values(by="date")
        # Int values
    df[col_ints] = df[col_ints].astype("Int64").fillna(pd.NA)
    return df


def main():
    source = "https://www.gov.mo/zh-hant/news/page"
    output_file = "automations/output/Macao.csv"
    last_update = pd.read_csv("automations/output/Macao.csv").date.max()
    df = read(source, last_update)
    if df is not None:
        df = df.pipe(pipeline)
        df = merge_with_current_data(df, output_file)
        df.to_csv(output_file, index=False)


if __name__ == "__main__":
    main()
