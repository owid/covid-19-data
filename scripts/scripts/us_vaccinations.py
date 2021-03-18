from datetime import datetime, timedelta
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
    df = pd.read_csv(path, na_values=[0.0, 0])

    # Each variable present in VARIABLE_MATCHING.keys() will be created based on the variables in
    # VARIABLE_MATCHING.values() by order of priority. If none of the vars can be found, the variable
    # is created as pd.NA
    variable_matching = {
        "total_distributed": ["Doses_Distributed"],
        "total_vaccinations": ["Doses_Administered"],
        "people_vaccinated": ["Administered_Dose1_Recip", "Administered_Dose1"],
        "people_fully_vaccinated": ["Series_Complete_Yes", "Administered_Dose2_Recip", "Administered_Dose2"],
    }

    for k,v in variable_matching.items():
        for cdc_variable in v:
            if cdc_variable in df.columns:
                df = df.rename(columns={cdc_variable: k})
                break
        if k not in df.columns:
            df[k] = pd.NA

    df = df[["Date", "LongName", "Census2019"] + [*variable_matching.keys()]]

    return df


def read_data():
    files = glob(os.path.join(INPUT_PATH, "cdc_data_*.csv"))
    data = [*map(read_file, files)]
    return pd.concat(data, ignore_index=True)


def rename_cols(df):
    col_dict = {
        "Date": "date",
        "LongName": "location",
    }
    return df.rename(columns=col_dict)


def add_per_capita(df):
    
    df["people_fully_vaccinated_per_hundred"] = df.people_fully_vaccinated.div(df.Census2019).mul(100)
    df["total_vaccinations_per_hundred"] = df.total_vaccinations.div(df.Census2019).mul(100)
    df["people_vaccinated_per_hundred"] = df.people_vaccinated.div(df.Census2019).mul(100)
    df["distributed_per_hundred"] = df.total_distributed.div(df.Census2019).mul(100)

    for var in df.columns:
        if "per_hundred" in var:
            df.loc[df[var].notnull(), var] = df[var].round(2)

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
    df = (
        read_data()
        .pipe(rename_cols)
        .pipe(add_per_capita)
        .pipe(add_smoothed)
        .pipe(add_usage)
        .drop(columns=["Census2019"]).sort_values(["location", "date"])
    )

    df = df[[
        "date", "location", "total_vaccinations", "total_distributed", "people_vaccinated",
        "people_fully_vaccinated_per_hundred", "total_vaccinations_per_hundred",
        "people_fully_vaccinated", "people_vaccinated_per_hundred", "distributed_per_hundred",
        "daily_vaccinations_raw", "daily_vaccinations", "daily_vaccinations_per_million",
        "share_doses_used",
    ]]

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
