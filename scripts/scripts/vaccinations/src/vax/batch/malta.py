import os

import pandas as pd


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source)


def check_columns(df: pd.DataFrame, expected) -> pd.DataFrame:
    n_columns = df.shape[1]
    if n_columns != expected:
        raise ValueError(
            "The provided input does not have {} columns. It has {} columns".format(
                expected, n_columns
            )
        )
    return df


def rename_columns(df: pd.DataFrame, columns: dict) -> pd.DataFrame:
    return df.rename(columns=columns)


def add_metrics(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(people_vaccinated=df.total_vaccinations - df.people_fully_vaccinated)


def format_date(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(date=pd.to_datetime(df.date, format="%d/%m/%Y").dt.date)


def enrich_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        location="Malta",
        source_url="https://github.com/COVID19-Malta/COVID19-Cases",
        vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech",
    )


def exclude_data_points(df: pd.DataFrame) -> pd.DataFrame:
    # The data contains an error that creates a negative change in the people_vaccinated series
    df = df[df.date.astype(str) != "2021-01-24"]
    return df


def pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(check_columns, expected=3)
        .pipe(rename_columns, columns={
            "Date": "date",
            "Total Vaccination Doses": "total_vaccinations",
            " Second Dose Taken": "people_fully_vaccinated",
        })
        .pipe(add_metrics)
        .pipe(format_date)
        .pipe(enrich_columns)
        .pipe(exclude_data_points)
    )


def main(paths):
    source = "https://github.com/COVID19-Malta/COVID19-Cases/raw/master/COVID-19%20Malta%20-%20Vaccination%20Data.csv"
    destination = paths.out_tmp("Malta")
    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
