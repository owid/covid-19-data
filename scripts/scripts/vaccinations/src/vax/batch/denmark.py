import os
import requests

import pandas as pd


def read(source: str) -> str:
    data = requests.get(source).json()
    return pd.DataFrame.from_records(elem["attributes"] for elem in data["features"])


def rename_columns(df: pd.DataFrame, colname: str) -> pd.DataFrame:
    df.columns = ("date", colname)
    return df


def format_date(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(date=pd.to_datetime(df.date, unit="ms"))


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .groupby("date")
        .sum()
        .sort_values("date")
        .cumsum()
        .reset_index()
    )


def enrich_vaccinations(df: pd.DataFrame) -> pd.DataFrame:
    df["people_vaccinated"] = df.people_vaccinated.ffill()
    df["people_fully_vaccinated"] = df.people_fully_vaccinated.ffill()
    return df.assign(
        total_vaccinations=df.people_vaccinated.fillna(0) + df.people_fully_vaccinated.fillna(0)
    )


def enrich_vaccine(df: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine(date: str) -> str:
        if date >= "2021-04-14":
            return "Moderna, Pfizer/BioNTech"
        if date >= "2021-02-08":
            return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
        if date >= "2021-01-13":
            return "Moderna, Pfizer/BioNTech"
        return "Pfizer/BioNTech"
    return df.assign(vaccine=df.date.astype(str).apply(_enrich_vaccine))


def enrich_metadata(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        location="Denmark",
        source_url="https://covid19.ssi.dk/overvagningsdata/vaccinationstilslutning"
    )


def pipeline(df: pd.DataFrame, colname: str) -> pd.DataFrame:
    return (
        df
        .pipe(rename_columns, colname)
        .pipe(format_date)
        .pipe(aggregate)
    )


def post_process(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .pipe(enrich_vaccinations)
        .pipe(enrich_vaccine)
        .pipe(enrich_metadata)
    )


def main(paths):
    source_dose1 = (
        "https://services5.arcgis.com/Hx7l9qUpAnKPyvNz/ArcGIS/rest/services/Vaccine_REG_linelist_gdb/FeatureServer/19/"
        "query?where=1%3D1&objectIds=&time=&resultType=none&outFields=first_vaccinedate%2Cantal_foerste_vacc&"
        "returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&"
        "cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&"
        "resultRecordCount=&sqlFormat=none&f=pjson&token="
    )
    source_dose2 = (
        "https://services5.arcgis.com/Hx7l9qUpAnKPyvNz/ArcGIS/rest/services/Vaccine_REG_linelist_gdb/FeatureServer/20/"
        "query?where=1%3D1&objectIds=&time=&resultType=none&outFields=second_vaccinedate%2Cantal_faerdig_vacc&"
        "returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&"
        "cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&"
        "resultRecordCount=&sqlFormat=none&f=pjson&token="
    )
    destination = paths.out_tmp("Denmark")

    dose1 = read(source_dose1).pipe(pipeline, colname="people_vaccinated")
    dose2 = read(source_dose2).pipe(pipeline, colname="people_fully_vaccinated")

    (
        pd.merge(dose1, dose2, how="outer", on="date")
        .pipe(post_process)
        .to_csv(destination, index=False)
    )


if __name__ == "__main__":
    main()
