import os
import sys
import gzip
from datetime import datetime
import pandas as pd
import pytz

from utils.db_imports import import_dataset

CURRENT_DIR = os.path.dirname(__file__)
sys.path.append(CURRENT_DIR)

URL = "https://raw.githubusercontent.com/govex/COVID-19/master/data_tables/JHU_USCountymap/df_Counties2020.csv"
DATASET_NAME = "JHU US County data"

INPUT_PATH = os.path.join(CURRENT_DIR, "../input/jhucounty/")
OUTPUT_PATH = os.path.join(CURRENT_DIR, "../grapher/")
TEMP_CSV_PATH = os.path.join(INPUT_PATH, "jhucountylatest.csv")
INPUT_CSV_GZIP_PATH = os.path.join(INPUT_PATH, "jhucountylatest.csv.gz")
OUTPUT_CSV_PATH = os.path.join(OUTPUT_PATH, DATASET_NAME + ".csv")

ZERO_DAY = "2020-01-01"
zero_day = datetime.strptime(ZERO_DAY, "%Y-%m-%d")


def download_csv():
    # Download the latest CSV
    os.system(f"curl --silent -f -o {TEMP_CSV_PATH} -L {URL}")
    # gzip in order to not exceed GitHub"s 100MB file limit
    with open(TEMP_CSV_PATH, "rb") as f_csv, gzip.open(INPUT_CSV_GZIP_PATH, "wb") as f_csv_gz:
        f_csv_gz.writelines(f_csv)
    os.remove(TEMP_CSV_PATH)


def export_grapher():

    county = pd.read_csv(INPUT_CSV_GZIP_PATH, compression="gzip", low_memory=False)
    county.drop(county.columns[county.columns.str.contains('unnamed', case=False)], axis=1, inplace=True)

    # Delete columns
    county = county.drop(columns=[
        "ST_ID"
    ])

    # Assign new column names
    rename_dict = {
        "Countyname": "County",
        "ST_Name": "State",
        "dt": "Date"
    }

    # Rename columns
    county = county.rename(columns=rename_dict)
    county['FIPS'] = county['FIPS'].astype(str).str.rjust(5, '0')
    # Save to files
    os.system("mkdir -p %s" % os.path.abspath(OUTPUT_PATH))
    county.to_csv(OUTPUT_CSV_PATH, index=False)


def update_db():
    time_str = datetime.now().astimezone(pytz.timezone("Europe/London")).strftime("%-d %B, %H:%M")
    source_name = f"JHU US County COVID-19 data – Last updated {time_str} (London time)"
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
