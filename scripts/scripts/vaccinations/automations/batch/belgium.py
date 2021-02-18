import os
import pandas as pd
import tempfile

from utils.pipeline import enrich_total_vaccinations


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source, usecols=["DATE", "DOSE", "COUNT"])


def aggregate(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.groupby(["DATE", "DOSE"], as_index=False)
        .sum()
        .sort_values("DATE")
        .pivot(index="DATE", columns="DOSE", values="COUNT")
        .reset_index()
    )


def rename_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.rename(
        columns={
            "DATE": "date",
            "A": "people_vaccinated",
            "B": "people_fully_vaccinated",
        }
    )


def add_totals(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        people_vaccinated=input.people_vaccinated.cumsum().ffill().fillna(0).astype(int),
        people_fully_vaccinated=input.people_fully_vaccinated.cumsum().ffill().fillna(0).astype(int),
    ).pipe(enrich_total_vaccinations)


def enrich_vaccine_name(input: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine_name(date: str) -> str:
        if date >= "2021-02-08":
            return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
        if date >= "2021-01-17":
            return "Moderna, Pfizer/BioNTech"
        return "Pfizer/BioNTech"

    return input.assign(vaccine=input.date.apply(_enrich_vaccine_name))


def enrich_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        location="Belgium",
        source_url="https://epistat.wiv-isp.be/covid/"
    )


def pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(aggregate)
        .pipe(rename_columns)
        .pipe(add_totals)
        .pipe(enrich_vaccine_name)
        .pipe(enrich_columns)
    )


def main():
    source = "https://epistat.sciensano.be/Data/COVID19BE_VACC.csv"
    destination = "automations/output/Belgium.csv"

    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
