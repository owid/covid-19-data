import datetime
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import vaxutils


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    for link in soup.find_all("a", class_="NEW"):
        if "Statistics for COVID-19 Vaccination Programme" in link.text:
            url = "https://www.info.gov.hk" + link["href"]
            break

    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    return pd.Series({
        "people_vaccinated": parse_people_vaccinated(soup),
        "source_url": url,
    })


def parse_people_vaccinated(soup: BeautifulSoup) -> int:
    regex = r"a cumulative total of about ([\d\s]+) persons have received the(ir)? first"
    people_vaccinated = re.search(regex, soup.find(id="pressrelease").text).group(1)
    return vaxutils.clean_count(people_vaccinated)


def add_metrics(ds: pd.Series) -> pd.Series:
    ds["total_vaccinations"] = ds["people_vaccinated"]
    return ds


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "location", "Hong Kong")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "vaccine", "Pfizer/BioNTech, Sinovac")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(add_metrics)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
    )


def main():
    date = datetime.date.today() - datetime.timedelta(days=1)
    source = f"https://www.info.gov.hk/gia/general/{date.strftime('%Y%m/%d')}.htm"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        date=str(date),
        source_url=data["source_url"],
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()
