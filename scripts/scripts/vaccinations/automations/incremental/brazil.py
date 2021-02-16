import vaxutils
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import datetime
import time
import pytz


def read(source: str) -> pd.Series:
    return connect_parse_data(source)


def connect_parse_data(source: str) -> pd.Series:
    op = Options()
    op.add_argument("--headless")

    with webdriver.Chrome(options=op) as driver:
        driver.get(source)
        time.sleep(10)
        for elem in driver.find_elements_by_class_name("kpimetric"):
            if "1ª Dose" in elem.text:
                people_vaccinated = elem.find_element_by_class_name("valueLabel").text
            elif "2ª Dose" in elem.text:
                people_fully_vaccinated = elem.find_element_by_class_name("valueLabel").text

    data = {'people_vaccinated': vaxutils.clean_count(people_vaccinated),
            'people_fully_vaccinated': vaxutils.clean_count(people_fully_vaccinated)}
    return pd.Series(data=data)


def format_date(input: pd.Series) -> pd.Series:
    date = str(datetime.datetime.now(pytz.timezone("Brazil/East")).date())
    return vaxutils.enrich_data(input, 'date', date)


def add_totals(input: pd.Series) -> pd.Series:
    total_vaccinations = int(input['people_vaccinated']) + int(input['people_fully_vaccinated'])
    return vaxutils.enrich_data(input, 'total_vaccinations', total_vaccinations)


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "Brazil")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine', "Oxford/AstraZeneca, Sinovac")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'source_url', "https://coronavirusbra1.github.io/")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(format_date)
            .pipe(add_totals)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://datastudio.google.com/embed/u/0/reporting/2f2537fa-ac23-4f08-8741-794cdbedca03/page/CPFTB"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=str(data['location']),
        total_vaccinations=int(data['total_vaccinations']),
        people_vaccinated=int(data['people_vaccinated']),
        people_fully_vaccinated=int(data['people_fully_vaccinated']),
        date=str(data['date']),
        source_url=str(data['source_url']),
        vaccine=str(data['vaccine'])
    )


if __name__ == "__main__":
    main()
