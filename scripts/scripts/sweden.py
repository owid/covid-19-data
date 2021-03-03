from datetime import datetime, timedelta
import os
import sys
import pandas as pd
import pytz
import numpy as np


CURRENT_DIR = os.path.dirname(__file__)
sys.path.append(CURRENT_DIR)

from utils.db_imports import import_dataset

DATASET_NAME = "COVID-19 - Swedish Public Health Agency"
INPUT_PATH = os.path.join(CURRENT_DIR, "../input/sweden/")
INPUT_FILE_PATH = os.path.join(INPUT_PATH, "sweden_deaths_per_day.csv")
GRAPHER_PATH = os.path.join(CURRENT_DIR, "../grapher/")
ZERO_DAY = "2020-01-01"
URL = "https://www.arcgis.com/sharing/rest/content/items/b5e7488e117749c19881cce45db13f7e/data"


def download_data():
    df = pd.read_excel(URL, sheet_name="Antal avlidna per dag")
    df.to_csv(INPUT_FILE_PATH, index=False)


def generate_dataset():
    df = pd.read_csv(INPUT_FILE_PATH, usecols=["Datum_avliden", "Antal_avlidna"])
    df = df.rename(columns={"Datum_avliden": "Date", "Antal_avlidna": "Deaths"})
    df = df[-df["Date"].str.contains("ppgift saknas")]
    df["Date"] = pd.to_datetime(df["Date"])
    assert len(df) > 100

    df = df.sort_values("Date")
    df.loc[:, "Deaths"] = df["Deaths"].rolling(7).mean().round(1)
    df.loc[df["Date"] >= (datetime.now() - timedelta(days=15)), "Incomplete deaths"] = df["Deaths"]
    df.loc[df["Date"] >= (datetime.now() - timedelta(days=14)), "Deaths"] = np.nan

    df["Country"] = "Sweden"
    df["Year"] = (df["Date"] - datetime(2020, 1, 1)).dt.days
    del df["Date"]

    df = df[["Country", "Year", "Deaths", "Incomplete deaths"]]
    df.to_csv(os.path.join(GRAPHER_PATH, f"{DATASET_NAME}.csv"), index=False)


def update_db():
    time_str = datetime.now().astimezone(pytz.timezone("Europe/London")).strftime("%-d %B, %H:%M")
    source_name = f"Swedish Public Health Agency – Last updated {time_str} (London time)"
    import_dataset(
        dataset_name=DATASET_NAME,
        namespace='owid',
        csv_path=os.path.join(GRAPHER_PATH, DATASET_NAME + ".csv"),
        default_variable_display={
            'yearIsDay': True,
            'zeroDay': ZERO_DAY
        },
        source_name=source_name,
        slack_notifications=False
    )


if __name__ == '__main__':
    download_data()
    generate_dataset()
