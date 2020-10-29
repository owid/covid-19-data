import datetime
import os
import pandas as pd


SOURCE_URL = "https://opendata.ecdc.europa.eu/covid19/hospitalicuadmissionrates/csv/data.csv"
CURRENT_DIR = os.path.dirname(__file__)
INPUT_PATH = os.path.join(CURRENT_DIR, "../input/")
OUTPUT_PATH = os.path.join(CURRENT_DIR, "../../public/data/ecdc/")
POPULATION = pd.read_csv(
    os.path.join(INPUT_PATH, "un/population_2020.csv"),
    usecols=["iso_code", "entity", "population"]
)


def download_data():
    df = pd.read_csv(SOURCE_URL, usecols=["country", "indicator", "date", "value", "year_week"])
    df = df.rename(columns={"country": "entity"})
    return df


def standardize_entities(df):
    df.loc[:, "entity"] = df["entity"].replace({
        "Czechia": "Czech Republic"
    })
    return df


def undo_per_100k(df):
    df = pd.merge(df, POPULATION, on="entity", how="left")
    assert df[df.population.isna()].shape[0] == 0, "Country missing from population file"
    df.loc[df["indicator"].str.contains(" per 100k"), "value"] = (
        df["value"].div(100000).mul(df["population"])
    )
    df.loc[:, "indicator"] = df["indicator"].str.replace(" per 100k", "")
    return df


def week_to_date(df):
    daily_records = df[df["indicator"].str.contains("Daily")]
    date_week_mapping = (
        daily_records[["year_week", "date"]].groupby("year_week", as_index=False).max()
    )
    weekly_records = df[df["indicator"].str.contains("Weekly")].drop(columns="date")
    weekly_records = pd.merge(weekly_records, date_week_mapping, on="year_week")
    df = pd.concat([daily_records, weekly_records]).drop(columns="year_week")
    return df


def add_per_million(df):
    per_million = df.copy()
    per_million.loc[:, "value"] = per_million["value"].div(per_million["population"]).mul(1000000)
    per_million.loc[:, "indicator"] = per_million["indicator"] + " per million"
    df = pd.concat([df, per_million]).drop(columns="population")
    return df


def owid_format(df):
    df.loc[:, "value"] = df["value"].round(3)
    df = df.drop(columns="iso_code")
    df = df.pivot(index=["entity", "date"], columns="indicator").value.reset_index()
    return df


def date_to_owid_year(df):
    df.loc[:, "date"] = (pd.to_datetime(df.date, format="%Y-%m-%d") - datetime.datetime(2020, 1, 21)).dt.days
    df = df.rename(columns={"date": "year"})
    return df


def main():
    df = download_data()
    df = standardize_entities(df)
    df = undo_per_100k(df)
    df = week_to_date(df)
    df = add_per_million(df)
    df = owid_format(df)
    df = date_to_owid_year(df)
    df.to_csv(os.path.join(OUTPUT_PATH, "COVID-2019 - Hospital & ICU (ECDC).csv"), index=False)


if __name__ == "__main__":
    main()
