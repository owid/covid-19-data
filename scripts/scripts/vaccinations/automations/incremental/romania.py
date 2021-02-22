import requests
from bs4 import BeautifulSoup
import pandas as pd
import tabula
import vaxutils


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    links = soup.find(class_="display-posts-listing").find_all("a", class_="title")

    for link in links:
        if "Actualizare zilnică" in link.text:
            url = link["href"]
            break

    soup = BeautifulSoup(requests.get(url).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    url = soup.find(class_="entry-content-text").find_all("a")[-1]["href"]

    kwargs = {'pandas_options': {'dtype': str, 'header': None}}
    dfs_from_pdf = tabula.read_pdf(url, pages="all", **kwargs)
    df = dfs_from_pdf[0]

    values = df[df[0] == "Total"].dropna()[2].str.split(" ")
    values = [vaxutils.clean_count(val) for val in pd.core.common.flatten(values)]
    assert len(values) == 2

    keys = ("date", "people_vaccinated", "people_fully_vaccinated", "source_url")
    values = (parse_date(soup), parse_people_vaccinated(values), parse_people_fully_vaccinated(values), url)
    data = dict(zip(keys, values))

    return pd.Series(data=data)


def parse_date(soup: BeautifulSoup) -> str:
    date = soup.find(class_="post-date").find(class_="meta-text").text.strip()
    date = vaxutils.clean_date(date, "%b %d, %Y")
    return date


def parse_people_vaccinated(values: pd.DataFrame) -> int:
    people_vaccinated = values[0] + values[1]
    return people_vaccinated


def parse_people_fully_vaccinated(values: pd.DataFrame) -> int:
    people_fully_vaccinated = values[1]
    return people_fully_vaccinated


def add_totals(ds: pd.Series) -> pd.Series:
    total_vaccinations = int(ds['people_fully_vaccinated']) + int(ds['people_vaccinated'])
    return vaxutils.enrich_data(ds, 'total_vaccinations', total_vaccinations)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'location', "Romania")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'vaccine', "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'source_url', "https://vaccinare-covid.gov.ro/comunicate-oficiale/")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds.pipe(add_totals)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
    )


def main():
    source = "https://vaccinare-covid.gov.ro/comunicate-oficiale/"
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
