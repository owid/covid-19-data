import datetime

import pandas as pd


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source)


def check_columns(input: pd.DataFrame, expected) -> pd.DataFrame:
    n_columns = input.shape[1]
    if n_columns != expected:
        raise ValueError(
            "The provided input does not have {} columns. It has {} columns".format(
                expected, n_columns
            )
        )
    return input


def select_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input[["date", "reported"]]


def rename_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.rename(columns={
        "reported": "total_vaccinations",
    })


def enrich_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        location="Netherlands",
        source_url="https://coronadashboard.rijksoverheid.nl/landelijk/vaccinaties",
    )


def add_vaccines(input: pd.DataFrame) -> pd.DataFrame:
    input = input.assign(vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")
    input.loc[input.date < "2021-02-08", "vaccine"] = "Moderna, Pfizer/BioNTech"
    input.loc[input.date < "2021-01-22", "vaccine"] = "Pfizer/BioNTech"
    return input


def pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(check_columns, expected=4)
        .pipe(select_columns)
        .pipe(rename_columns)
        .pipe(enrich_columns)
        .pipe(add_vaccines)
    )


def main():
    # Using @Sikerdebaard's scraped data for now - official JSONs are too unstable
    source = "https://github.com/Sikerdebaard/netherlands-vaccinations-scraper/raw/main/vaccine_administered_total.csv"
    destination = "automations/output/Netherlands.csv"
    data = read(source).pipe(pipeline)

    assert (datetime.datetime.now() - pd.to_datetime(data.date.max())).days < 3, \
    "External repository is not up to date"

    data.to_csv(destination, index=False)


if __name__ == "__main__":
    main()
