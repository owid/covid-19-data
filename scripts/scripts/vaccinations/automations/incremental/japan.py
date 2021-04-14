import datetime

import pandas as pd

import vaxutils


def read(source: str) -> pd.Series:

    df = pd.read_excel(source, skiprows=2, usecols="A:E", nrows=2)

    first_col = df.columns[0]

    total_vaccinations = df.loc[df[first_col] == "合計", " 接種回数　"].item()
    people_vaccinated = df.loc[df[first_col] == "合計", " 内１回目"].item()
    people_fully_vaccinated = df.loc[df[first_col] == "合計", " 内２回目"].item()
    date = str(df.loc[df[first_col] != "合計", first_col].item().date())
    
    return pd.Series(data={
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "date": date,
    })


def merge_series(healthcare: pd.Series, elderly: pd.Series) -> pd.Series:
    return pd.Series(data={
        "total_vaccinations": healthcare.total_vaccinations + elderly.total_vaccinations,
        "people_vaccinated": healthcare.people_vaccinated + elderly.people_vaccinated,
        "people_fully_vaccinated": healthcare.people_fully_vaccinated + elderly.people_fully_vaccinated,
        "date": max([healthcare.date, elderly.date]),
    })


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "location", "Japan")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, "vaccine", "Pfizer/BioNTech")


def enrich_source(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(
        ds, "source_url", "https://www.kantei.go.jp/jp/headline/kansensho/vaccine.html"
    )


def pipeline(healthcare: pd.Series, elderly: pd.Series) -> pd.Series:
    return (
        merge_series(healthcare, elderly)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main():

    source_healthcare = "https://www.kantei.go.jp/jp/content/IRYO-vaccination_data.xlsx"
    source_elderly = "https://www.kantei.go.jp/jp/content/KOREI-vaccination_data.xlsx"

    data = pipeline(read(source_healthcare), read(source_elderly))

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
