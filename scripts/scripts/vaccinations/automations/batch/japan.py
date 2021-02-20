import requests

from bs4 import BeautifulSoup
import pandas as pd


def read(source: str) -> pd.DataFrame:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    table = soup.find(class_="l-contentMain").find("table")
    df = pd.read_html(str(table))[0]
    df.columns = df.iloc[0, :]
    df = df[df["日付"].str.contains(r"[\d/]{10}", regex=True)]
    df = df.drop(columns=["施設数(*)"])
    return df


def check_columns(input: pd.DataFrame, expected) -> pd.DataFrame:
    n_columns = input.shape[1]
    if n_columns != expected:
        raise ValueError(
            "The provided input does not have {} columns. It has {} columns".format(
                expected, n_columns
            )
        )
    return input


def rename_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.rename(columns={
        "日付": "date",
        "接種回数": "total_vaccinations",
        "内１回目": "people_vaccinated",
        "内２回目": "people_fully_vaccinated"
    })


def calculate_metrics(input: pd.DataFrame) -> pd.DataFrame:
    return input.sort_values("date").assign(
        total_vaccinations=input.total_vaccinations.astype(int).cumsum(),
        people_vaccinated=input.people_vaccinated.astype(int).cumsum(),
        people_fully_vaccinated=input.people_fully_vaccinated.astype(int).cumsum(),
    )


def format_date(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(date=pd.to_datetime(input.date, format="%Y/%m/%d").dt.date)


def enrich_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        location="Japan",
        source_url="https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/vaccine_sesshujisseki.html",
        vaccine="Pfizer/BioNTech",
    )


def pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input
        .pipe(check_columns, expected=4)
        .pipe(rename_columns)
        .pipe(calculate_metrics)
        .pipe(format_date)
        .pipe(enrich_columns)
    )


def main():
    source = "https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/vaccine_sesshujisseki.html"
    destination = "automations/output/Japan.csv"
    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
