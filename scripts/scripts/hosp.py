import datetime
import json
import os
import requests
import numpy as np
import pandas as pd


SOURCE_URL = "https://opendata.ecdc.europa.eu/covid19/hospitalicuadmissionrates/csv/data.csv"
CURRENT_DIR = os.path.dirname(__file__)
INPUT_PATH = os.path.join(CURRENT_DIR, "../input/")
OUTPUT_PATH = os.path.join(CURRENT_DIR, "../grapher/")
POPULATION = pd.read_csv(
    os.path.join(INPUT_PATH, "un/population_2020.csv"),
    usecols=["iso_code", "entity", "population"]
)


def download_data():
    print("Downloading ECDC data…")
    df = pd.read_csv(SOURCE_URL, usecols=["country", "indicator", "date", "value", "year_week"])
    df = df.drop_duplicates()
    df = df.rename(columns={"country": "entity"})
    return df


def standardize_entities(df):
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


def add_united_states(df):
    print("Downloading US data…")
    url = "https://healthdata.gov/api/views/g62h-syeh/rows.csv"

    usa = pd.read_csv(url, usecols=[
        "date",
        "total_adult_patients_hospitalized_confirmed_covid",
        "total_pediatric_patients_hospitalized_confirmed_covid",
        "staffed_icu_adult_patients_confirmed_covid",
        "previous_day_admission_adult_covid_confirmed",
        "previous_day_admission_pediatric_covid_confirmed",
    ])

    usa.loc[:, "date"] = pd.to_datetime(usa.date)
    usa = usa[usa.date >= pd.to_datetime("2020-07-15")]
    usa = usa.groupby("date", as_index=False).sum()

    stock = usa[[
        "date",
        "total_adult_patients_hospitalized_confirmed_covid",
        "total_pediatric_patients_hospitalized_confirmed_covid",
        "staffed_icu_adult_patients_confirmed_covid",
    ]].copy()
    stock.loc[:, "Daily hospital occupancy"] = (
        stock.total_adult_patients_hospitalized_confirmed_covid
        .add(stock.total_pediatric_patients_hospitalized_confirmed_covid)
    )
    stock = stock.rename(columns={
        "staffed_icu_adult_patients_confirmed_covid": "Daily ICU occupancy"
    })
    stock = stock[["date", "Daily hospital occupancy", "Daily ICU occupancy"]]
    stock = stock.melt(id_vars="date", var_name="indicator")
    stock.loc[:, "date"] = stock["date"].dt.date

    flow = usa[[
        "date",
        "previous_day_admission_adult_covid_confirmed",
        "previous_day_admission_pediatric_covid_confirmed",
    ]].copy()
    flow.loc[:, "value"] = (
        flow.previous_day_admission_adult_covid_confirmed
        .add(flow.previous_day_admission_pediatric_covid_confirmed)
    )
    flow.loc[:, "date"] = (
        (flow["date"] + pd.to_timedelta(6 - flow["date"].dt.dayofweek, unit="d")).dt.date
    )
    flow = flow[flow["date"] <= datetime.date.today()]
    flow = flow[["date", "value"]]
    flow = flow.groupby("date", as_index=False).sum()
    flow.loc[:, "indicator"] = "Weekly new hospital admissions"

    # Merge all subframes
    usa = pd.concat([stock, flow])

    usa.loc[:, "entity"] = "United States"
    usa.loc[:, "iso_code"] = "USA"
    usa.loc[:, "population"] = 331002647

    df = pd.concat([df, usa])
    return df


def add_canada(df):
    print("Downloading Canada data…")
    url = "https://api.covid19tracker.ca/reports?after=2020-03-09"
    data = requests.get(url).json()
    data = json.dumps(data["data"])
    canada = pd.read_json(data, orient="records")
    canada = canada[["date", "total_hospitalizations", "total_criticals"]]
    canada = canada.melt("date", ["total_hospitalizations", "total_criticals"], "indicator")
    canada.loc[:, "indicator"] = canada["indicator"].replace({
        "total_hospitalizations": "Daily hospital occupancy",
        "total_criticals": "Daily ICU occupancy"
    })

    canada.loc[:, "date"] = canada["date"].dt.date
    canada.loc[:, "entity"] = "Canada"
    canada.loc[:, "iso_code"] = "CAN"
    canada.loc[:, "population"] = 37742157

    df = pd.concat([df, canada])
    return df


def add_uk(df):
    print("Downloading UK data…")
    url = "https://api.coronavirus.data.gov.uk/v2/data?areaType=overview&metric=hospitalCases&metric=newAdmissions&metric=covidOccupiedMVBeds&format=csv"
    uk = pd.read_csv(url, usecols=["date", "hospitalCases", "newAdmissions", "covidOccupiedMVBeds"])
    uk.loc[:, "date"] = pd.to_datetime(uk["date"])

    stock = uk[["date", "hospitalCases", "covidOccupiedMVBeds"]].copy()
    stock = stock.melt("date", var_name="indicator")
    stock.loc[:, "date"] = stock["date"].dt.date

    flow = uk[["date", "newAdmissions"]].copy()
    flow.loc[:, "date"] = (flow["date"] + pd.to_timedelta(6 - flow["date"].dt.dayofweek, unit="d")).dt.date
    flow = flow[flow["date"] <= datetime.date.today()]
    flow = flow.groupby("date", as_index=False).sum()
    flow = flow.melt("date", var_name="indicator")

    uk = pd.concat([stock, flow]).dropna(subset=["value"])
    uk.loc[:, "indicator"] = uk["indicator"].replace({
        "hospitalCases": "Daily hospital occupancy",
        "covidOccupiedMVBeds": "Daily ICU occupancy",
        "newAdmissions": "Weekly new hospital admissions",
    })

    uk.loc[:, "entity"] = "United Kingdom"
    uk.loc[:, "iso_code"] = "GBR"
    uk.loc[:, "population"] = 67886004

    df = pd.concat([df, uk])
    return df


def add_israel(df):
    print("Downloading Israel data…")
    url = "https://datadashboardapi.health.gov.il/api/queries/patientsPerDate"
    israel = pd.read_json(url)
    israel.loc[:, "date"] = pd.to_datetime(israel["date"])

    stock = israel[["date", "Counthospitalized", "CountCriticalStatus"]].copy()
    stock.loc[:, "date"] = stock["date"].dt.date
    stock.loc[stock["date"].astype(str) < "2020-08-17", "CountCriticalStatus"] = np.nan
    stock = stock.melt("date", var_name="indicator")

    flow = israel[["date", "new_hospitalized", "serious_critical_new"]].copy()
    flow.loc[:, "date"] = (flow["date"] + pd.to_timedelta(6 - flow["date"].dt.dayofweek, unit="d")).dt.date
    flow = flow[flow["date"] <= datetime.date.today()]
    flow = flow.groupby("date", as_index=False).sum()
    flow = flow.melt("date", var_name="indicator")

    israel = pd.concat([stock, flow]).dropna(subset=["value"])
    israel.loc[:, "indicator"] = israel["indicator"].replace({
        "Counthospitalized": "Daily hospital occupancy",
        "CountCriticalStatus": "Daily ICU occupancy",
        "new_hospitalized": "Weekly new hospital admissions",
        "serious_critical_new": "Weekly new ICU admissions"
    })

    israel.loc[:, "entity"] = "Israel"
    israel.loc[:, "iso_code"] = "ISR"
    israel.loc[:, "population"] = 8655541

    return pd.concat([df, israel])



def add_countries(df):
    df = add_united_states(df)
    df = add_canada(df)
    df = add_uk(df)
    df = add_israel(df)
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

    # Data cleaning
    df = df[-df["indicator"].str.contains("Weekly new plot admissions")]
    df = df.groupby(["entity", "date", "indicator"], as_index=False).max()

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
    df = add_countries(df)
    df = add_per_million(df)
    df = owid_format(df)
    df = date_to_owid_year(df)
    df.to_csv(os.path.join(OUTPUT_PATH, "COVID-2019 - Hospital & ICU.csv"), index=False)


if __name__ == "__main__":
    main()
