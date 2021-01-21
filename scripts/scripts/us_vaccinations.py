from datetime import datetime
from glob import glob
import json
import os
import requests
import sys
import pandas as pd
import pytz


CURRENT_DIR = os.path.dirname(__file__)
sys.path.append(CURRENT_DIR)

from utils.db_imports import import_dataset

DATASET_NAME = "COVID-19 - United States vaccinations"
INPUT_PATH = os.path.join(CURRENT_DIR, "vaccinations/us_states/input/")
GRAPHER_PATH = os.path.join(CURRENT_DIR, "../grapher/")
OUTPUT_PATH = os.path.join(CURRENT_DIR, "../../public/data/vaccinations/")
ZERO_DAY = "2021-01-01"


def download_data():
    url = "https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=vaccination_data"
    data = json.loads(requests.get(url).content)
    df = pd.DataFrame.from_records(data["vaccination_data"])
    assert len(df) > 0
    df.to_csv(os.path.join(INPUT_PATH, f"cdc_data_{df.Date.max()}.csv"), index=False)


def read_file(path):
    cols = [
        "Date", "LongName", "Doses_Distributed", "Doses_Administered", "Dist_Per_100K",
        "Admin_Per_100K", "Administered_Dose1", "Administered_Dose1_Per_100K",
        "Administered_Dose2", "Administered_Dose2_Per_100K", "Census2019"
    ]
    df = pd.read_csv(path, na_values=[0.0, 0], usecols=cols)
    return df


def read_data():
    files = glob(os.path.join(INPUT_PATH, "cdc_data_*.csv"))
    df = pd.concat(map(read_file, files)).reset_index(drop=True)
    return df


def change_capita_base(df, from_base, to_base, from_suffix, to_suffix):
    for col in df.columns:
        if from_suffix in col:
            df[col] = df[col].div(from_base).mul(to_base).round(2)
            df = df.rename(columns={col: col.replace(from_suffix, to_suffix)})
    return df


def fill_missing_values(df):
    df.loc[(df["Dist_per_hundred"].isna()) & (df["Census2019"].notnull()), "Dist_per_hundred"] = (
        df["Doses_Distributed"].div(df["Census2019"]).mul(100).round(2)
    )
    df.loc[(df["Admin_per_hundred"].isna()) & (df["Census2019"].notnull()), "Admin_per_hundred"] = (
        df["Doses_Administered"].div(df["Census2019"]).mul(100).round(2)
    )
    df.loc[(df["Administered_Dose1_per_hundred"].isna()) & (df["Census2019"].notnull()), "Administered_Dose1_per_hundred"] = (
        df["Administered_Dose1"].div(df["Census2019"]).mul(100).round(2)
    )
    df.loc[(df["Administered_Dose2_per_hundred"].isna()) & (df["Census2019"].notnull()), "Administered_Dose2_per_hundred"] = (
        df["Administered_Dose2"].div(df["Census2019"]).mul(100).round(2)
    )
    return df


def rename_cols(df):
    col_dict = {
        "Date": "date",
        "LongName": "location",
        "Doses_Distributed": "total_distributed",
        "Doses_Administered": "total_vaccinations",
        "Dist_per_hundred": "distributed_per_hundred",
        "Admin_per_hundred": "total_vaccinations_per_hundred",
        "Administered_Dose1": "people_vaccinated",
        "Administered_Dose1_per_hundred": "people_vaccinated_per_hundred",
        "Administered_Dose2": "people_fully_vaccinated",
        "Administered_Dose2_per_hundred": "people_fully_vaccinated_per_hundred",
    }
    df = df.rename(columns=col_dict)
    return df


def add_smoothed_state(df):
    df = df.set_index("date").resample("1D").asfreq().reset_index().sort_values("date")
    df[["location", "Census2019"]] = df[["location", "Census2019"]].ffill()
    interpolated_totals = df["total_vaccinations"].interpolate("linear")
    df["daily_vaccinations_raw"] = interpolated_totals - interpolated_totals.shift(1)
    df["daily_vaccinations"] = df["daily_vaccinations_raw"].rolling(7, min_periods=1).mean().round()
    df["daily_vaccinations_per_million"] = df["daily_vaccinations"].mul(1000000).div(df["Census2019"]).round()
    return df


def add_smoothed(df):
    df = df.sort_values(["date", "location"])
    df["date"] = pd.to_datetime(df["date"])
    df = df.groupby("location", as_index=False).apply(add_smoothed_state)
    return df


def add_usage(df):
    df["share_doses_used"] = df["total_vaccinations"].div(df["total_distributed"]).round(3)
    return df


def sanity_checks(df):
    assert len(df) == len(df[["date", "location"]].drop_duplicates())


def export_to_public(df):
    df.to_csv(os.path.join(OUTPUT_PATH, "us_state_vaccinations.csv"), index=False)


def export_to_grapher(df):
    df = df.rename(columns={"date": "Year", "location": "Country"})
    df["Year"] = (pd.to_datetime(df["Year"], format="%Y-%m-%d") - datetime(2021, 1, 1)).dt.days
    df.insert(0, "Country", df.pop("Country"))
    df.to_csv(os.path.join(GRAPHER_PATH, "COVID-19 - United States vaccinations.csv"), index=False)


def generate_dataset():
    df = read_data()
    df = change_capita_base(df, 100000, 100, "Per_100K", "per_hundred")
    df = fill_missing_values(df)
    df = rename_cols(df)
    df = add_smoothed(df)
    df = add_usage(df)
    df = df.drop(columns=["Census2019"]).sort_values(["location", "date"])
    sanity_checks(df)
    export_to_public(df.copy())
    export_to_grapher(df.copy())


def update_db():
    time_str = (datetime.now() - timedelta(minutes=10)).astimezone(pytz.timezone("US/Eastern")).strftime("%B %-d, %H:%M")
    source_name = f"Centers for Disease Control and Prevention – Last updated {time_str} (Eastern Time)"
    import_dataset(
        dataset_name=DATASET_NAME,
        namespace='owid',
        csv_path=os.path.join(GRAPHER_PATH, DATASET_NAME + ".csv"),
        default_variable_display={
            'yearIsDay': True,
            'zeroDay': ZERO_DAY
        },
        source_name=source_name
    )


if __name__ == '__main__':
    download_data()
    generate_dataset()
