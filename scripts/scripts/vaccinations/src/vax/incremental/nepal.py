import tempfile
import re
from datetime import datetime

import requests
import pandas as pd
import PyPDF2
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from vax.utils.incremental import clean_count, enrich_data, increment


def read(source: str):
    url_pdf = parse_pdf_link(source)
    pdf_text = get_text_from_pdf(url_pdf)
    return pd.Series({
        "people_vaccinated": parse_people_vaccinated(pdf_text),
        "date": parse_date(pdf_text),
        "source_url": url_pdf
    })


def parse_pdf_link(url: str) -> str:
    op = Options()
    op.add_argument("--headless")
    with webdriver.Chrome(options=op) as driver:
        driver.get(url)
        a = driver.find_elements_by_tag_name("a")
        a = [aa for aa in a if aa.text == "Download Situation Report"]
        if len(a) > 1:
            raise Exception("Format changed")
        url_pdf = a[0].get_attribute("href")
    return url_pdf


def get_text_from_pdf(url_pdf: str) -> str:
    with tempfile.NamedTemporaryFile() as tf:
        with open(tf.name, mode="wb") as f:
            f.write(requests.get(url_pdf).content)
        with open(tf.name, mode="rb") as f:
            reader = PyPDF2.PdfFileReader(f)
            page = reader.getPage(0)
            text = page.extractText().replace("\n", "")
    return text


def parse_date(pdf_text: str):
    regex = r"Updated as of (\d+)(.+)(20\d+)"
    day = clean_count(re.search(regex, pdf_text).group(1))
    month = _get_month(re.search(regex, pdf_text).group(2))
    year = clean_count(re.search(regex, pdf_text).group(3))
    return datetime(year, month, day).strftime("%Y-%m-%d")


def parse_people_vaccinated(pdf_text: str):
    regex = r"(\d+)[ ]?Total people vaccinated"
    return clean_count(re.search(regex, pdf_text).group(1))


def _get_month(month_raw: str):
    months_dix = {
        "January": 1,
        "February": 2,
        "March": 3,
        "April": 4,
        "May": 5,
        "June": 6,
        "July": 7,
        "August": 8,
        "September": 9,
        "October": 10,
        "November": 11,
        "December": 12
    }
    for month_name, month_id in months_dix.items():
        if month_name in month_raw:
            return month_id


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Nepal")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Sinopharm/Beijing")


def add_totals(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "total_vaccinations", ds.people_vaccinated)


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(add_totals)
    )


def main():
    source = "https://covid19.mohp.gov.np/"
    data = read(source).pipe(pipeline)
    increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()
