import vaxutils
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time


def read(source: str) -> pd.Series:
    return connect_parse_data(source)


def connect_parse_data(source: str) -> pd.Series:
    op = Options()
    op.add_argument("--headless")

    with webdriver.Chrome(options=op) as driver:
        driver.get(source)
        time.sleep(5)

        total_vaccinations = driver.find_element_by_id("counter1").text

        date = driver.find_element_by_id("pupdateddate").text
        date = vaxutils.clean_date(date.replace("Updated ", ""), "%d %b, %Y")

    data = {
        "total_vaccinations": vaxutils.clean_count(total_vaccinations),
        "date": date,
    }
    return pd.Series(data=data)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "location", "Qatar")


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
    source = "https://covid19.moph.gov.qa/EN/Pages/default.aspx"
    data = read(source).pipe(pipeline, source)
    vaxutils.increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()
