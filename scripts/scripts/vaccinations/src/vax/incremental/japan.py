import datetime

import pandas as pd
import pytz

from vax.utils.incremental import enrich_data, increment, clean_count
from vax.utils.utils import get_soup


def read(source: str) -> pd.Series:

    soup = get_soup(source)
    blocks = soup.find_all(class_="aly_tx_center")

    for block in blocks:

        if "医療従事者等：" in block.text:
            healthcare_workers = clean_count(block.find("font").text)

        elif "高齢者：" in block.text:
            elderly = clean_count(block.find("font").text)

    total_vaccinations = healthcare_workers + elderly
    
    return pd.Series(data={
        "total_vaccinations": total_vaccinations,
    })


def enrich_date(input: pd.Series) -> pd.Series:
    date = str(datetime.datetime.now(pytz.timezone("Asia/Tokyo")).date())
    return enrich_data(input, "date", date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Japan")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Pfizer/BioNTech")


def enrich_source(ds: pd.Series, source: str) -> pd.Series:
    return enrich_data(ds, "source_url", source)


def pipeline(ds: pd.Series, source: str) -> pd.Series:
    return (
        ds
        .pipe(enrich_date)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source, source)
    )


def main():
    source = "https://www.kantei.go.jp/jp/headline/kansensho/vaccine.html"
    data = read(source).pipe(pipeline, source)
    increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()
