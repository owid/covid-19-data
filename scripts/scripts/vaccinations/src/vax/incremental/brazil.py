import os
import datetime
import pytz

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from vax.utils.incremental import enrich_data, increment, clean_count


def read(source: str) -> pd.Series:
    return connect_parse_data(source)


def connect_parse_data(source: str) -> pd.Series:
    op = Options()
    op.add_argument("--headless")

    with webdriver.Chrome(options=op) as driver:
        driver.get(source)
        data_blocks = (
            WebDriverWait(driver, 20)
            .until(EC.visibility_of_all_elements_located((By.CLASS_NAME, "sn-kpi-data")))
        )
        for block in data_blocks:
            block_title = block.find_element_by_class_name("sn-kpi-measure-title").get_attribute("title")
            if block_title == "Pessoas Vacinadas (Dose 1)":
                people_vaccinated = block.find_element_by_class_name("sn-kpi-value").text
            elif block_title == "Pessoas Vacinadas (Dose 2)":
                people_fully_vaccinated = block.find_element_by_class_name("sn-kpi-value").text
            elif block_title == "Doses Aplicadas":
                total_vaccinations = block.find_element_by_class_name("sn-kpi-value").text

    return pd.Series({
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
    }).transform(clean_count)


def set_date(ds: pd.Series) -> pd.Series:
    date = str((datetime.datetime.now(pytz.timezone("Brazil/East")) - datetime.timedelta(days=1)).date())
    return enrich_data(ds, "date", date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Brazil")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Sinovac")


def enrich_source(ds: pd.Series, source: str) -> pd.Series:
    return enrich_data(ds, "source_url", source)


def pipeline(ds: pd.Series, source: str) -> pd.Series:
    return (
        ds
        .pipe(set_date)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source, source)
    )


def main(paths):
    source = "https://qsprod.saude.gov.br/extensions/DEMAS_C19Vacina/DEMAS_C19Vacina.html"
    data = read(source).pipe(pipeline, source)
    increment(
        paths=paths,
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
