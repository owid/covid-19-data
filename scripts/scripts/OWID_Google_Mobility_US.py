import os
import sys
import urllib.request
import pandas as pd

CURRENT_DIR = os.path.dirname(__file__)
sys.path.append(CURRENT_DIR)

URL = "https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv"

INPUT_PATH = os.path.join(CURRENT_DIR, "../input/GoogleMobilityUS/")
OUTPUT_PATH = os.path.join(CURRENT_DIR, "../../public/data/Mobility/")
TEMP_CSV_PATH = os.path.join(INPUT_PATH, "latest.csv")
OUTPUT_PATH = os.path.join(OUTPUT_PATH, "Google Mobility Report US County Level.csv")


def get_mobility_data():
    """Download Google Mobility data in CSV format
    """
    # create directory if it doesn't exist
    if not os.path.exists(INPUT_PATH) and INPUT_PATH != '':
        os.makedirs(INPUT_PATH)
    else:
        files = os.listdir(INPUT_PATH)
        for file in files:
            os.remove(INPUT_PATH + '/' + file)

    urllib.request.urlretrieve(URL, os.path.join(TEMP_CSV_PATH))


def build_report():
    """Build cleaned Google report
    """
    mobilityData = pd.read_csv(os.path.join(TEMP_CSV_PATH), low_memory=False)
    mobilityData.columns = mobilityData.columns.str.replace(
        r'_percent_change_from_baseline', '')
    mobilityData.columns = mobilityData.columns.str.replace(r'_', ' ')
    mobilityData = mobilityData.rename(columns={'country region': 'country'})
    mobilityData = mobilityData[(mobilityData['country'] == "United States")]
    mobilityData = mobilityData.rename(
        columns={
            'sub region 1': 'state',
            'sub region 2': 'county',
            'census fips code': 'fips code'})
    mobilityData = mobilityData.loc[:,
                   ['state',
                    'county',
                    'fips code',
                    'date',
                    'retail and recreation',
                    'grocery and pharmacy',
                    'parks',
                    'transit stations',
                    'workplaces',
                    'residential']]

    mobilityData.dropna(subset=['county'], how='all', inplace=True)
    mobilityData.fillna(0, inplace=True)
    mobilityData['fips code'] = mobilityData['fips code'].astype(str).replace('\.0', '', regex=True)
    mobilityData['fips code'] = mobilityData['fips code'].str.rjust(5, '0')
    return mobilityData


if __name__ == '__main__':
    # get Google data
    get_mobility_data()
    # build reports
    mobilityData_US = build_report()
    # create csv
    mobilityData_US.to_csv(OUTPUT_PATH, index=False)
