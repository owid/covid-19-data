import datetime

import pandas as pd
import pytz

import vaxutils


def read(source: str) -> pd.Series:
    return connect_parse_data(source)


def connect_parse_data(source: str) -> pd.Series:

    soup = vaxutils.get_soup(source)

    people_vaccinated = soup.find(class_="count-up").text
    people_vaccinated = vaxutils.clean_count(people_vaccinated)

    total_vaccinations = people_vaccinated

    return pd.Series(data={
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
    })


def enrich_date(input: pd.Series) -> pd.Series:
    date = str(datetime.datetime.now(pytz.timezone("Europe/Skopje")).date() - datetime.timedelta(days=1))
    return vaxutils.enrich_data(input, "date", date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "location", "North Macedonia")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "vaccine", "Pfizer/BioNTech")


def enrich_source(ds: pd.Series, source: str) -> pd.Series:
    return vaxutils.enrich_data(ds, "source_url", source)


def pipeline(ds: pd.Series, source: str) -> pd.Series:
    return (
        ds
        .pipe(enrich_date)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source, source)
    )


def main():
    source = "https://kovid19vakcinacija.mk/"
    data = read(source).pipe(pipeline, source)
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
