import json
import requests
import datetime
import pandas as pd


SOURCE_URL = "https://services7.arcgis.com/1Cyg6S9yGgIqdFPO/ArcGIS/rest/services/cases_today/FeatureServer/0/query"


def kenya_get_tests_snapshot():
    url = SOURCE_URL
    params = {
        'f': 'json',
        'where': '1=1',
        'returnGeometry': False,
        'spatialRel': 'esriSpatialRelIntersects',
        'outFields': '*',
        'resultOffset': 0,
        'resultRecordCount': 1,
        'resultType': 'standard',
        'cacheHint': True
    }
    res = requests.get(url, params=params)
    json_data = json.loads(res.text)
    assert len(json_data['features']) == 1, "only expected a single result."
    timestamp = json_data['features'][0]['attributes']['Date']
    date = datetime.datetime.utcfromtimestamp(timestamp/1000).strftime('%Y-%m-%d')
    tests_cumul = int(json_data['features'][0]['attributes']['total_test_samples'])
    tests_today = int(json_data['features'][0]['attributes']['daily_test_samples'])
    return date, tests_cumul, tests_today


def main():
    date, tests_cumul, tests_today = kenya_get_tests_snapshot()

    existing = pd.read_csv("automated_sheets/Kenya.csv")

    if date > existing["Date"].max():

        new = pd.DataFrame({
            "Country": "Kenya",
            "Date": [date],
            "Cumulative total": tests_cumul,
            "Source URL": SOURCE_URL,
            "Source label": "Kenya Ministry of Health",
            "Testing type": "PCR only",
            "Units": "samples tested",
            "Notes": pd.NA
        })

        df = pd.concat([new, existing]).sort_values("Date", ascending=False)
        df.to_csv("automated_sheets/Kenya.csv", index=False)


if __name__ == '__main__':
    main()
