from datetime import datetime

import pandas as pd
import tabula

from vax.utils.utils import get_soup
from vax.utils.incremental import clean_count, enrich_data, increment


def read(source: str) -> pd.Series:
    url = f"{source}/Category/Page/9jFXNbCe-sFK9EImRRi2Og"
    soup = get_soup(url)
    url_pdf = parse_pdf_link(source, soup)
    df = parse_table(url_pdf)
    return pd.Series({
        "total_vaccinations": parse_total_vaccinations(df),
        "people_vaccinated": parse_total_vaccinations(df),
        "date": parse_date(df)
    })

def parse_pdf_link(base_url: str, soup) -> str:
    a = soup.find(class_="download").find("a")
    url_pdf = f"{base_url}{a['href']}"
    soup = get_soup(url_pdf)
    a = soup.find(class_="nav-link viewer-button")
    return f"{base_url}{a['href']}"


def parse_table(url_pdf: str) -> int:
    kwargs = {"pandas_options": {"dtype": str, "header": None}}
    dfs_from_pdf = tabula.read_pdf(url_pdf, pages="all", **kwargs)
    df = dfs_from_pdf[1]
    df = df.dropna(subset=[2])
    return df


def parse_total_vaccinations(df: pd.DataFrame) -> int:
    return clean_count(df.iloc[-1, 3])


def parse_date(df: pd.DataFrame) -> str:
    date_str = datetime.strptime(df.iloc[0, 2], "%m/%d接種數").strftime("2021-%m-%d")
    return date_str


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Taiwan")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
    )


def main():
    source = "https://www.cdc.gov.tw"
    data = read(source).pipe(pipeline)
    increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        date=data["date"],
        source_url=f"{source}/Category/Page/9jFXNbCe-sFK9EImRRi2Og",
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()
