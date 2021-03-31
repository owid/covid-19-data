import datetime

import pandas as pd
import pytz

import vaxutils


def read(source: str) -> pd.Series:
    return connect_parse_data(source)


def connect_parse_data(source: str) -> pd.Series:

    soup = vaxutils.get_soup(source)
    tables = pd.read_html(str(soup))

    for table in tables:

        if table.iloc[0, 0] == "عدد متلقي اللقاح":
            people_vaccinated = vaxutils.clean_count(table.iloc[1, 0])

        elif table.iloc[0, 0] == "عدد المطعمين بشكل كامل":
            people_fully_vaccinated = vaxutils.clean_count(table.iloc[1, 0])

        elif table.iloc[0, 0] == "عدد الجرعات":
            total_vaccinations = vaxutils.clean_count(table.iloc[1, 0])
    
    return pd.Series(data={
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
    })


def enrich_date(input: pd.Series) -> pd.Series:
    date = str(datetime.datetime.now(pytz.timezone("Asia/Jerusalem")).date() - datetime.timedelta(days=1))
    return vaxutils.enrich_data(input, "date", date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "location", "Palestine")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "vaccine", "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


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
    source = "https://corona.ps/details"
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
