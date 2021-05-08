import os
import re
import requests
import tempfile

from bs4 import BeautifulSoup
import pandas as pd
import PyPDF2

from vax.utils.incremental import enrich_data, increment, clean_date, clean_count


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
    
    covishield_data = re.search(r"Covishield Vaccine (\d+) (\d+)", text)
    covishield_dose1 = clean_count(covishield_data.group(1))
    covishield_dose2 = clean_count(covishield_data.group(2))
    
    sinopharm_data = re.search(r"Sinopharm Vaccine \(Chinese Nationals\) (\d+) (\d+)", text)
    sinopharm_dose1 = clean_count(sinopharm_data.group(1))
    sinopharm_dose2 = clean_count(sinopharm_data.group(2))

    total_vaccinations = covishield_dose1 + covishield_dose2 + sinopharm_dose1 + sinopharm_dose2
    people_vaccinated = covishield_dose1 + sinopharm_dose1
    people_fully_vaccinated = covishield_dose2 + sinopharm_dose2

    regex = r"Situation Report\s+([\d\.]{10})"
    date = re.search(regex, text).group(1)
    date = clean_date(date, "%d.%m.%Y")

    return pd.Series(data={
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "date": date,
        "source_url": pdf_path,
    })


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Sri Lanka")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Sinopharm/Beijing")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
    )


def main(paths):
    source = "https://www.epid.gov.lk/web/index.php?option=com_content&view=article&id=225&lang=en"
    data = read(source).pipe(pipeline)
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
