import json
import requests
import datetime
import pandas as pd


SOURCE_URL = "https://gis.rbc.gov.rw/server/rest/services/Hosted/service_b580a3db9319449e82045881f1667b01/FeatureServer/0/query"


def rwanda_get_tests_snapshot():
    url = SOURCE_URL
    params = {
        'f': 'json',
        'where': '1=1',
        'returnGeometry': False,
        'spatialRel': 'esriSpatialRelIntersects',
        'orderByFields': 'last_edited_date desc',
        'outFields': '*',
        'resultRecordCount': 1,
        'resultType': 'standard',
        'cacheHint': True
    }
    res = requests.get(url, params=params)
    json_data = json.loads(res.text)
    assert len(json_data['features']) == 1, "only expected a single result."
    timestamp = json_data['features'][0]['attributes']['last_edited_date']
    date = datetime.datetime.utcfromtimestamp(timestamp/1000).strftime('%Y-%m-%d')
    tests_cumul = int(json_data['features'][0]['attributes']['cumulative_test'])
    tests_today = int(json_data['features'][0]['attributes']['sample_tested'])
    return date, tests_cumul, tests_today


def main():
    date, tests_cumul, tests_today = rwanda_get_tests_snapshot()

    existing = pd.read_csv("automated_sheets/Rwanda.csv")

    if date > existing["Date"].max():

        new = pd.DataFrame({
            "Country": "Rwanda",
            "Date": [date],
            "Cumulative total": tests_cumul,
            "Source URL": SOURCE_URL,
            "Source label": "Rwanda Ministry of Health",
            "Testing type": "PCR only",
            "Units": "samples tested",
            "Notes": pd.NA
        })

        df = pd.concat([new, existing]).sort_values("Date", ascending=False)
        df.to_csv("automated_sheets/Rwanda.csv", index=False)


if __name__ == '__main__':
    main()
