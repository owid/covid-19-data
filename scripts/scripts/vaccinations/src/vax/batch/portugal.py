import os

import pandas as pd


def read(source_continent: str, source_islands: str) -> pd.DataFrame:

    # vaccinas.csv: contains daily vaccination data, extracted from the DGS dashboard.
    # Note: these values, like the dashboard and images published on social networks,
    # correspond only to the population residing on the continent, excluding the islands.
    df_continent = pd.read_csv(source_continent, usecols=["data", "doses", "doses1", "doses2"])

    # vaccinas_detalhe.csv: contains detailed weekly data on vaccination, extracted from the DGS
    # vaccination report dataset. Starting on 17-03-2021 it includes vaccination data for islands.
    df_islands = pd.read_csv(source_islands, usecols=[
        "data", "doses_açores", "doses1_açores", "doses2_açores",
        "doses_madeira", "doses1_madeira", "doses2_madeira"
    ])

    # If the daily data from vacinas.csv ever includes the islands directly, we'll need to remove
    # this merge and simply use the data from vacinas.csv
    df = pd.merge(df_continent, df_islands, how="outer", on="data")

    return df


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={"data": "date"})


def format_date(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        date=pd.to_datetime(df.date, format="%d-%m-%Y").astype(str)
    )


def calculate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("date").ffill().fillna(0)
    return (
        df
        .assign(
            total_vaccinations=df.doses + df.doses_madeira + df.doses_açores,
            people_vaccinated=df.doses1 + df.doses1_madeira + df.doses1_açores,
            people_fully_vaccinated=df.doses2 + df.doses2_madeira + df.doses2_açores,
        )
        [["date", "total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]]
    )


def enrich_vaccine_name(df: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine_name(date: str) -> str:
        if date >= "2021-02-09":
            return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
        return "Pfizer/BioNTech"

    return df.assign(vaccine=df.date.apply(_enrich_vaccine_name))


def enrich_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        location="Portugal",
        source_url="https://github.com/dssg-pt/covid19pt-data"
    )


def pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .pipe(rename_columns)
        .pipe(format_date)
        .pipe(calculate_metrics)
        .pipe(enrich_vaccine_name)
        .pipe(enrich_columns)
        .sort_values("date")
    )


def main(paths):
    source_continent = "https://github.com/dssg-pt/covid19pt-data/raw/master/vacinas.csv"
    source_islands = "https://github.com/dssg-pt/covid19pt-data/raw/master/vacinas_detalhe.csv"
    destination = paths.out_tmp("Portugal")

    read(source_continent, source_islands).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
