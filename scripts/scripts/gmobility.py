import os
import sys
from datetime import datetime
import pandas as pd
import pytz
from utils.db_imports import import_dataset

CURRENT_DIR = os.path.dirname(__file__)
sys.path.append(CURRENT_DIR)

URL = "https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv"
DATASET_NAME = "Google Mobility Trends (2020)"

INPUT_PATH = os.path.join(CURRENT_DIR, "../input/gmobility/")
OUTPUT_PATH = os.path.join(CURRENT_DIR, "../grapher/")
INPUT_CSV_PATH = os.path.join(INPUT_PATH, "latest.csv")
OUTPUT_CSV_PATH = os.path.join(OUTPUT_PATH, f"{DATASET_NAME}.csv")

ZERO_DAY = "2020-01-01"
zero_day = datetime.strptime(ZERO_DAY, "%Y-%m-%d")

def download_csv():
    # Download the latest CSV
    os.system(f"curl --silent -f -o {INPUT_CSV_PATH} -L {URL}")

def export_grapher():

    cols = [
        "country_region",
        "sub_region_1",
        "sub_region_2",
        "metro_area",
        "iso_3166_2_code",
        "census_fips_code",
        "date",
        "retail_and_recreation_percent_change_from_baseline",
        "grocery_and_pharmacy_percent_change_from_baseline",
        "parks_percent_change_from_baseline",
        "transit_stations_percent_change_from_baseline",
        "workplaces_percent_change_from_baseline",
        "residential_percent_change_from_baseline"
    ]

    mobility = pd.read_csv(INPUT_CSV_PATH, usecols=cols, low_memory=False)

    # Convert date column to days since zero_day
    mobility["date"] = pd.to_datetime(
        mobility["date"],
        format="%Y/%m/%d"
    ).map(
        lambda date: (date - zero_day).days
    )

    # Standardise country names to OWID country names
    country_mapping = pd.read_csv(os.path.join(INPUT_PATH, "gmobility_country_standardized.csv"))
    mobility = country_mapping.merge(mobility, on="country_region")

    # Remove subnational data, keeping only country figures
    filter_cols = [
        "sub_region_1",
        "sub_region_2",
        "metro_area",
        "iso_3166_2_code",
        "census_fips_code"
    ]
    country_mobility = mobility[mobility[filter_cols].isna().all(1)]

    # Delete columns
    country_mobility = country_mobility.drop(columns=[
        "country_region",
        "sub_region_1",
        "sub_region_2",
        "metro_area",
        "census_fips_code",
        "iso_3166_2_code"
    ])

    # Assign new column names
    rename_dict = {
        "date": "Year",
        "retail_and_recreation_percent_change_from_baseline": "retail_and_recreation",
        "grocery_and_pharmacy_percent_change_from_baseline": "grocery_and_pharmacy",
        "parks_percent_change_from_baseline": "parks",
        "transit_stations_percent_change_from_baseline": "transit_stations",
        "workplaces_percent_change_from_baseline": "workplaces",
        "residential_percent_change_from_baseline": "residential"
    }

    # Rename columns
    country_mobility = country_mobility.rename(columns=rename_dict)

    # Replace time series with 7-day rolling averages
    country_mobility = country_mobility.sort_values(by=["Country", "Year"]).reset_index(drop=True)
    smoothed_cols = [
        "retail_and_recreation", "grocery_and_pharmacy", "parks",
        "transit_stations", "workplaces", "residential"
    ]
    country_mobility[smoothed_cols] = (
        country_mobility
        .groupby("Country", as_index=False)
        .rolling(window=7, min_periods=3, center=False)
        .mean()
        .round(3)
        .reset_index()[smoothed_cols]
    )

    # Save to files
    os.system("mkdir -p %s" % os.path.abspath(OUTPUT_PATH))
    country_mobility.to_csv(OUTPUT_CSV_PATH, index=False)

def update_db():
    time_str = datetime.now().astimezone(pytz.timezone("Europe/London")).strftime("%-d %B, %H:%M")
    source_name = f"Google COVID-19 Community Mobility Trends – Last updated {time_str} (London time)"
    import_dataset(
        dataset_name=DATASET_NAME,
        namespace="owid",
        csv_path=OUTPUT_CSV_PATH,
        default_variable_display={
            "yearIsDay": True,
            "zeroDay": ZERO_DAY
        },
        source_name=source_name
    )

if __name__ == "__main__":
    download_csv()
    export_grapher()
