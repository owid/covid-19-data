import vaxutils
import datetime
import pytz
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

        for elem in driver.find_elements_by_class_name("col-lg-12"):
            if "TOTAL DOZE ADMINISTRATE" in elem.text:
                parent_elem = elem.parent
                total_vaccinations = parent_elem.find_element_by_tag_name("span").text
            if "PERSOANE VACCINATE" in elem.text:
                parent_elem = elem.parent
                people_vaccinated = parent_elem.find_element_by_tag_name("span").text

        data = {'people_vaccinated': vaxutils.clean_count(people_vaccinated),
                'total_vaccinations': vaxutils.clean_count(total_vaccinations)
                }
    return pd.Series(data=data)


def add_totals(ds: pd.Series) -> pd.Series:
    people_fully_vaccinated = ds['total_vaccinations'] - ds['people_vaccinated']
    return vaxutils.enrich_data(ds, 'people_fully_vaccinated', people_fully_vaccinated)


def format_date(ds: pd.Series) -> pd.Series:
    date = str(datetime.datetime.now(pytz.timezone("Europe/Chisinau")).date())
    return vaxutils.enrich_data(ds, 'date', date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'location', "Moldova")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'vaccine', "Oxford/AstraZeneca")


def enrich_source(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'source_url', "https://vaccinare.gov.md/")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds.pipe(add_totals)
            .pipe(format_date)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://vaccinare.gov.md/"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=data['location'],
        total_vaccinations=data['total_vaccinations'],
        people_vaccinated=data['people_vaccinated'],
        people_fully_vaccinated=data['people_fully_vaccinated'],
        date=data['date'],
        source_url=data['source_url'],
        vaccine=data['vaccine']
    )


if __name__ == "__main__":
    main()
