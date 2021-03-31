import datetime
import requests

from bs4 import BeautifulSoup
import pandas as pd
import pytz
import tabula

import vaxutils


def read(source: str) -> pd.Series:

    soup = BeautifulSoup(requests.get(source).content, "html.parser")

    for img in soup.find(id="site-dashboard").find_all("img"):
        if img["alt"] == "Vaccination State Data":
            url = img.parent["href"]
            break

    soup = BeautifulSoup(requests.get(url).content, "html.parser")
    return parse_data(url)


def parse_data(url: str) -> pd.Series:
    
    kwargs = {"pandas_options": {"dtype": str, "header": None}}
    dfs_from_pdf = tabula.read_pdf(url, pages="all", **kwargs)
    for df in dfs_from_pdf:
        if "Beneficiaries vaccinated" in dfs_from_pdf[0].values.flatten():
            break
    df = df[df[0] == "India"]
    ncols = df.shape[1]

    people_vaccinated = vaxutils.clean_count(df[ncols-3].item())
    people_fully_vaccinated = vaxutils.clean_count(df[ncols-2].item())
    total_vaccinations = vaxutils.clean_count(df[ncols-1].item())

    return pd.Series({
        "date": str((datetime.datetime.now(pytz.timezone("Asia/Kolkata")) - datetime.timedelta(days=1)).date()),
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "total_vaccinations": total_vaccinations,
        "source_url": url,
    })


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "location", "India")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "vaccine", "Covaxin, Oxford/AstraZeneca")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
    )


def main():
    source = "https://www.mohfw.gov.in/"
    data = read(source).pipe(pipeline)
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
