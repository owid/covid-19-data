import pandas as pd
import vaxutils
import datetime
import os
import re
import tabula


def read(source: str) -> pd.Series:
    return parse_data(source)


def parse_data(source: str) -> pd.Series:
    os.system(f"curl {source} -o morocco.pdf -s")
    dfs = tabula.read_pdf("morocco.pdf", pages=1, pandas_options={"dtype": str, "header": None})

    df = dfs[0]
    data = {
        "people_fully_vaccinated": vaxutils.clean_count(df[0].values[-1]),
        "people_vaccinated": vaxutils.clean_count(df[1].values[-1]),
    }
    data["total_vaccinations"] = data["people_vaccinated"] + data["people_fully_vaccinated"]
    return pd.Series(data=data)


def format_date(input: pd.Series) -> pd.Series:
    date = datetime.date.today() - datetime.timedelta(days=1)
    date = str(date)
    return vaxutils.enrich_data(input, "date", date)


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "location", "Morocco")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "vaccine", "Oxford/AstraZeneca, Sinopharm/Beijing")


def enrich_source(input: pd.Series, source: str) -> pd.Series:
    return vaxutils.enrich_data(input, "source_url", source)


def pipeline(input: pd.Series, source: str) -> pd.Series:
    return (
        input.pipe(format_date)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source, source)
    )


def main():
    dt = datetime.date.today() - datetime.timedelta(days=1)
    url_date = dt.strftime("%-d.%-m.%y")
    source = f"http://www.covidmaroc.ma/Documents/BULLETIN/{url_date}.COVID-19.pdf"
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
    os.remove("morocco.pdf")


if __name__ == "__main__":
    main()
