import tempfile
import re
from datetime import datetime

import requests
import pandas as pd
from bs4 import BeautifulSoup
import PyPDF2
from pdfreader import SimplePDFViewer

import vaxutils


def read(source: str):
    soup = vaxutils.get_soup(source)
    url = parse_pdf_link(soup, source)
    ds = pd.Series(parse_data(url))
    return ds


def parse_pdf_link(soup: BeautifulSoup, source: str):
    href = soup.find("a", string="Vaksinasiya").get("href")
    return f"{source}{href}"


def parse_data(source_pdf: str):
    with tempfile.NamedTemporaryFile() as tf:
        with open(tf.name, mode="wb") as f:
            f.write(requests.get(source_pdf).content)
        total_vaccinations, people_vaccinated, people_fully_vaccinated = parse_vaccinations(tf.name)
        date = parse_date(tf.name)
    return {
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "date": date
    }

def parse_date(filename):
    # Read pdf (for date)
    with open(filename, mode="rb") as f:
        reader = PyPDF2.PdfFileReader(f)
        page = reader.getPage(0)
        text = page.extractText()
    # Get date
    date_str = re.search(r"\n(?P<count>\d{1,2}.\d{1,2}.\d{4})\n", text).group(1)
    return datetime.strptime(date_str, "%d.%m.%Y").strftime("%Y-%m-%d")


def parse_vaccinations(filename):
    # Read pdf (for metrics)
    with open(filename, mode="rb") as f:
        viewer = SimplePDFViewer(f)
        viewer.render()
    # Get list with strings
    strs = viewer.canvas.strings
    # Get indices
    idx_total_vax = strs.index("ümumi sayı")
    idx_dose_1 = strs.index("1-ci mərhələ üzrə ")
    idx_dose_2 = strs.index("2-ci mərhələ üzrə ")
    # Get metrics
    total_vaccinations = max([int(s) for s in strs[idx_total_vax:idx_dose_1] if s.isnumeric()])
    dose_1 = max([int(s) for s in strs[idx_dose_1:idx_dose_2] if s.isnumeric()])
    dose_2 = max([int(s) for s in strs[idx_dose_2:] if s.isnumeric()])
    # Sanity check
    if dose_1 + dose_2 != total_vaccinations:
        raise ValueError(f"Apparently, dose_1 + dose_2 != total_vaccinations ({dose_1} + {dose_2} != {total_vaccinations})")
    return total_vaccinations, dose_1, dose_2


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "location", "Azerbaijan")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "vaccine", "Sinovac")


def enrich_source(ds: pd.Series, source: str) -> pd.Series:
    return vaxutils.enrich_data(ds, "source_url", source)


def pipeline(df: pd.DataFrame, source: str) -> pd.DataFrame:
    return (
        df
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source, source)
    )


def main():
    source = "https://koronavirusinfo.az"
    data = read(source).pipe(pipeline, source)
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