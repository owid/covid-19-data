import os
import sys
import gzip
import urllib.request
import json
from datetime import datetime
import pandas as pd
import pytz

from utils.db_imports import import_dataset

CURRENT_DIR = os.path.dirname(__file__)
sys.path.append(CURRENT_DIR)

URL = "https://raw.githubusercontent.com/govex/COVID-19/master/data_tables/JHU_USCountymap/df_Counties2020.csv"
DATASET_NAME = "Apple Mobility Trends"

INPUT_PATH = os.path.join(CURRENT_DIR, "../input/amobility/")
OUTPUT_PATH = os.path.join(CURRENT_DIR, "../grapher/")
TEMP_CSV_PATH = os.path.join(INPUT_PATH, "amobilitylatest.csv")
INPUT_CSV_GZIP_PATH = os.path.join(INPUT_PATH, "amobilitylatest.csv.gz")
OUTPUT_CSV_PATH = os.path.join(OUTPUT_PATH, DATASET_NAME + ".csv")

ZERO_DAY = "2020-01-01"
zero_day = datetime.strptime(ZERO_DAY, "%Y-%m-%d")


def download_csv():
    jlink = "https://covid19-static.cdn-apple.com/covid19-mobility-data/current/v3/index.json"
    with urllib.request.urlopen(jlink) as url:
        jdata = json.loads(url.read().decode())
    link = "https://covid19-static.cdn-apple.com" + \
           jdata['basePath'] + jdata['regions']['en-us']['csvPath']

    # Download the latest CSV
    os.system(f"curl --silent -f -o {TEMP_CSV_PATH} -L {link}")
    # gzip in order to not exceed GitHub"s 100MB file limit
    with open(TEMP_CSV_PATH, "rb") as f_csv, gzip.open(INPUT_CSV_GZIP_PATH, "wb") as f_csv_gz:
        f_csv_gz.writelines(f_csv)
    os.remove(TEMP_CSV_PATH)


def export_grapher():

    amobility = pd.read_csv(INPUT_CSV_GZIP_PATH, compression="gzip", low_memory=False)
    amobility['country'] = amobility.apply(
        lambda x: x['region'] if x['geo_type'] == 'country/region' else x['country'],
        axis=1)
    amobility = amobility.loc[amobility['geo_type'] == 'country/region']
    print(amobility.head)

    # Delete columns
    amobility = amobility.drop(columns=[
        'alternative_name', 'region', 'sub-region'
    ])

    amobility = amobility.melt(
        id_vars=[
            'geo_type',
            'transportation_type',
            'country'],
        var_name='date')
    amobility['value'] = amobility['value'] - 100

    amobility = amobility.pivot_table(
        index=[
            "geo_type",
            "date",
            "country"],
        columns='transportation_type').reset_index()
    amobility.columns = [t + (v if v != "value" else "")
                     for v, t in amobility.columns]
    amobility = amobility.loc[:,
            ['country',
             'date',
             'driving',
             'transit',
             'walking']]
    amobility = amobility.sort_values(by=['country', 'date']).reset_index(drop=True)
    amobility.fillna(0, inplace=True)

    # Save to files
    os.system("mkdir -p %s" % os.path.abspath(OUTPUT_PATH))
    amobility.to_csv(OUTPUT_CSV_PATH, index=False)


def update_db():
    time_str = datetime.now().astimezone(pytz.timezone("Europe/London")).strftime("%-d %B, %H:%M")
    source_name = f"Apple Mobility Trends – Last updated {time_str} (London time)"
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
