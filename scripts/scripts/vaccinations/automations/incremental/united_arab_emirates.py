import requests
import pandas as pd
from bs4 import BeautifulSoup
import vaxutils
import datetime
import pytz


    total_vaccinations = soup.find(class_="doses").find(class_="counter").text
    return vaxutils.clean_count(total_vaccinations)


def format_date(input: pd.Series) -> pd.Series:
    date = str(datetime.datetime.now(pytz.timezone("Asia/Dubai")).date() - datetime.timedelta(days=1))
    return vaxutils.enrich_data(input, 'date', date)


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "United Arab Emirates")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine',
                                "Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sinopharm/Wuhan, Sputnik V")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'source_url',
                                "http://covid19.ncema.gov.ae/en")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(format_date)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "http://covid19.ncema.gov.ae/en"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=str(data['location']),
        total_vaccinations=int(data['total_vaccinations']),
        date=str(data['date']),
        source_url=str(data['source_url']),
        vaccine=str(data['vaccine'])
    )


if __name__ == "__main__":
    main()
