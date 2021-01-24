import os
import sys
import urllib.request
import json
import pandas as pd


site_url = "https://covid19-static.cdn-apple.com"
index_url = "https://covid19-static.cdn-apple.com/covid19-mobility-data/current/v3/index.json"

CURRENT_DIR = os.path.dirname(__file__)
sys.path.append(CURRENT_DIR)

INPUT_PATH = os.path.join(CURRENT_DIR, "../input/AppleMobilityUS/")
OUTPUT_PATH = os.path.join(CURRENT_DIR, "../../public/data/Mobility/")
TEMP_CSV_PATH = os.path.join(INPUT_PATH, "latest.csv")
OUTPUT_PATH = os.path.join(OUTPUT_PATH, "Apple Mobility Trends Report US County Level.csv")


def get_mobility_link():
    """Get Apple Mobility data link
    """

    # get link
    with urllib.request.urlopen(index_url) as url:
        json_link = json.loads(url.read().decode())
        base_path = json_link['basePath']
        csv_path = json_link['regions']['en-us']['csvPath']
        link = site_url + \
               base_path + csv_path
    return link


def get_mobility_data():
    """Download Apple Mobility data in CSV format
    """
    # create directory if it doesn't exist
    if not os.path.exists(INPUT_PATH) and INPUT_PATH != '':
        os.makedirs(INPUT_PATH)

    if os.path.exists(TEMP_CSV_PATH):
        os.remove(TEMP_CSV_PATH)

    urllib.request.urlretrieve(get_mobility_link(), TEMP_CSV_PATH)


def build_report():
    """Build cleaned mobility data
    """
    mobilityData = pd.read_csv(TEMP_CSV_PATH, low_memory=False)
    mobilityData = mobilityData.drop(columns=['alternative_name'])
    mobilityData['country'] = mobilityData.apply(
        lambda x: x['region'] if x['geo_type'] == 'country/region' else x['country'],
        axis=1)

    mobilityData = mobilityData[mobilityData.country == "United States"].drop(columns=[
        'country'])
    mobilityData['sub-region'] = mobilityData['sub-region'].fillna(
        mobilityData['region']).replace({"United States": "Total"})
    mobilityData['region'] = mobilityData.apply(lambda x: x['region'] if (
            x['geo_type'] == 'city' or x['geo_type'] == 'county') else 'Total', axis=1)
    mobilityData = mobilityData.rename(
        columns={
            'sub-region': 'state',
            'region': 'county'})

    mobilityData = mobilityData.melt(
        id_vars=[
            'geo_type',
            'state',
            'county',
            'transportation_type'],
        var_name='date')
    mobilityData['value'] = mobilityData['value'] - 100

    mobilityData = mobilityData.pivot_table(
        index=[
            'geo_type',
            'state',
            'county',
            'date'],
        columns='transportation_type').reset_index()
    mobilityData.columns = [t + (v if v != "value" else "")
                            for v, t in mobilityData.columns]

    mobilityData = mobilityData.loc[:, ['state', 'county', 'geo_type',
                                        'date', 'driving', 'transit', 'walking']]
    mobilityData = mobilityData.sort_values(
        by=['state', 'county', 'geo_type', 'date']).reset_index(drop=True)
    mobilityData = mobilityData[(mobilityData['geo_type'] == "county")]
    mobilityData.fillna(0, inplace=True)

    fipsData = pd.read_csv(INPUT_PATH + "/county_fipscode.csv")
    fipsData['f_county_country'] = fipsData['county name'].astype(str) + ',' + fipsData['state name'].astype(str)
    mobilityData['m_county_country'] = mobilityData['county'].astype(str) + ',' + mobilityData['state'].astype(
        str)

    mergeData = pd.merge(mobilityData, fipsData, left_on=['m_county_country'],
                         right_on=['f_county_country'],
                         how='left', sort=False)

    mergeData = mergeData.drop(columns=['m_county_country', 'state name', 'county name', 'f_county_country', 'geo_type'])
    mergeData['fips code'] = mergeData['fips code'].astype(str).replace('\.0', '', regex=True)
    mergeData['fips code'] = mergeData['fips code'].str.rjust(5, '0')
    mergeData.fillna(0, inplace=True)
    return mergeData


if __name__ == '__main__':

    # get data
    get_mobility_data()
    # build reports
    mobilityData_US = build_report()
    # create csv
    mobilityData_US.to_csv(OUTPUT_PATH, index=False)

