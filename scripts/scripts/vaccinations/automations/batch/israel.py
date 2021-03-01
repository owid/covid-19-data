import datetime
import json
import requests
import pandas as pd
from utils.pipeline import enrich_total_vaccinations


def read(source: str) -> pd.DataFrame:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.16; rv:85.0) Gecko/20100101 Firefox/85.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
    }
    data = json.loads(requests.get(source, headers=headers).content)
    return pd.DataFrame.from_records(data)


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "Day_Date": "date",
            "vaccinated_cum": "people_vaccinated",
            "vaccinated_seconde_dose_cum": "people_fully_vaccinated"
        }
    )


def format_date(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(date=df.date.str.slice(0, 10))


def filter_date(df: pd.DataFrame) -> pd.DataFrame:
    return df[df.date < str(datetime.date.today())]


def select_distinct(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(["people_vaccinated", "people_fully_vaccinated"], as_index=False).min()


def enrich_source(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        source_url="https://datadashboard.health.gov.il/COVID-19/general"
    )


def enrich_location(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        location="Israel",
    )


def enrich_vaccine(df: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine(date: str) -> str:
        if date >= "2021-01-07":
            return "Moderna, Pfizer/BioNTech"
        return "Pfizer/BioNTech"
    return df.assign(
        vaccine=df.date.apply(_enrich_vaccine)
    )


def format_nulls_as_nans(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        people_fully_vaccinated=df.people_fully_vaccinated.replace(0, pd.NA)
    )


def select_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = (
        df[["date", "total_vaccinations", "people_vaccinated",
        "people_fully_vaccinated", "location", "source_url", "vaccine"
        ]]
    )
    return df


def pre_process(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(rename_columns)
        .pipe(format_date)
        .pipe(filter_date)
        .pipe(select_distinct)
    )


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(enrich_total_vaccinations)
        .pipe(enrich_location)
        .pipe(enrich_source)
        .pipe(enrich_vaccine)
    )


def post_process(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(format_nulls_as_nans)
        .pipe(select_output_columns)
    )


def pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(pre_process)
        .pipe(enrich)
        .pipe(post_process)
    )


def main():
    source = "https://datadashboardapi.health.gov.il/api/queries/vaccinated"
    destination = "automations/output/Israel.csv"

    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
