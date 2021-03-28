import re
import requests

from bs4 import BeautifulSoup
import pandas as pd
import PyPDF2
import tempfile

import vaxutils


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:

    # Get path to newest pdf
    links = soup.find(class_="rt-article").find_all("a")
    for link in links:
        if "sitrep-sl-en" in link["href"]:
            pdf_path = "https://www.epid.gov.lk" + link["href"]
            break

    tf = tempfile.NamedTemporaryFile()

    with open(tf.name, mode="wb") as f:
        f.write(requests.get(pdf_path).content)

    with open(tf.name, mode="rb") as f:
        reader = PyPDF2.PdfFileReader(f)
        page = reader.getPage(0)
        text = page.extractText().replace("\n", "")

    regex = r"COVID-19\s+Total\s+Vaccinated\s+(\d+)"
    total_vaccinations = re.search(regex, text).group(1)
    total_vaccinations = vaxutils.clean_count(total_vaccinations)

    people_vaccinated = total_vaccinations

    regex = r"Situation Report\s+([\d\.]{10})"
    date = re.search(regex, text).group(1)
    date = vaxutils.clean_date(date, "%d.%m.%Y")

    return pd.Series(data={
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "date": date,
        "source_url": pdf_path,
    })


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "location", "Sri Lanka")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "vaccine", "Oxford/AstraZeneca")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
    )


def main():
    source = "https://www.epid.gov.lk/web/index.php?option=com_content&view=article&id=225&lang=en"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()
