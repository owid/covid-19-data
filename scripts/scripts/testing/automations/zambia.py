"""Constructs daily time series of COVID-19 testing data for Zambia.
ArcGIS Dashboard: https://zambia-open-data-nsdi-mlnr.hub.arcgis.com/pages/zambia-covid19
"""

import re
import json
import datetime
import requests
import pandas as pd

COUNTRY = 'Zambia'
UNITS = 'tests performed'
TESTING_TYPE = 'PCR only'
SOURCE_LABEL = 'Government of Zambia'
SOURCE_URL = 'https://zambia-open-data-nsdi-mlnr.hub.arcgis.com/pages/zambia-covid19'
DATA_URL = 'https://services9.arcgis.com/ZNWWwa7zEkUIYLEA/arcgis/rest/services/service_d73fa15b0b304945a52e048ed42028a9/FeatureServer/0/query'
PARAMS = {
    'f': 'json',
    'where': "reportdt>=timestamp '2020-01-01 00:00:00'",
    'returnGeometry': False,
    'spatialRel': 'esriSpatialRelIntersects',
    'outFields': '*',
    'orderByFields': 'reportdt asc',
    'resultOffset': 0,
    'resultRecordCount': 32000,
    'resultType': 'standard',
    'cacheHint': True,
}

# sample of official values for cross-checking against the API data.
official_cumulative_totals = [
    ("2020-09-03", {"cumulative_total": 119567, "source": "https://twitter.com/mohzambia/status/1301477446936678400"}),
    ("2020-08-08", {"cumulative_total": 93344, "source": "https://twitter.com/mohzambia/status/1292086483978014722"}),
    ("2020-08-06", {"cumulative_total": 90307, "source": "https://twitter.com/mohzambia/status/1291398767959322629"}),
    ("2020-07-30", {"cumulative_total": 81482, "source": "https://www.facebook.com/mohzambia/posts/1656823021159015"}),
    ("2020-07-29", {"cumulative_total": 80239, "source": "https://www.facebook.com/mohzambia/posts/1655960864578564"}),
    ("2020-07-28", {"cumulative_total": 79269, "source": "https://www.facebook.com/mohzambia/posts/1654985441342773"}),
    ("2020-05-07", {"cumulative_total": 11412, "source": "https://twitter.com/mohzambia/status/1258424347011756033"}),
    ("2020-04-30", {"cumulative_total": 6828, "source": "http://znphi.co.zm/news/wp-content/uploads/2020/05/Zambia_COVID-Situational-Report-No-43_30April20_Final.pdf"}),
    ("2020-04-09", {"cumulative_total": 1314, "source": "http://znphi.co.zm/news/wp-content/uploads/2020/04/Zambia_COVID-Situational-Report-No-22_09April20_Final.pdf"}),
    ("2020-04-08", {"cumulative_total": 1222, "source": "http://znphi.co.zm/news/wp-content/uploads/2020/04/Zambia_COVID-Situational-Report-No-21_08April20_Final.pdf"}),
    ("2020-04-07", {"cumulative_total": 1087, "source": "http://znphi.co.zm/news/wp-content/uploads/2020/04/Zambia_COVID-Situational-Report-No-20_07April20_Final.pdf"}),
    ("2020-03-31", {"cumulative_total": 520, "source": "http://znphi.co.zm/news/wp-content/uploads/2020/04/Zambia_COVID-Situational-Report-No-13_310320_Final.pdf"}),
    ("2020-03-22", {"cumulative_total": 75, "source": "http://znphi.co.zm/news/wp-content/uploads/2020/03/Zambia_COVID-Situational-Report-No-4_220320_final.pdf"}),
]

def main() -> None:
    df = get_data()
    df = df.sort_values('Date')
    df['Country'] = COUNTRY
    df['Units'] = UNITS
    df['Testing type'] = TESTING_TYPE
    df['Source URL'] = SOURCE_URL
    df['Source label'] = SOURCE_LABEL
    df['Notes'] = ""
    sanity_checks(df)
    df = df[['Country', 'Units', 'Testing type', 'Date', 'Cumulative total', 'Source URL', 'Source label', 'Notes']]
    df.to_csv("automated_sheets/Zambia.csv", index=False)

def get_data() -> pd.DataFrame:
    res = requests.get(DATA_URL, params=PARAMS)
    assert res.ok
    json_data = json.loads(res.text)
    df = pd.DataFrame([feat['attributes'] for feat in json_data['features']])
    df['reportdt'] = df['reportdt'].astype(int).apply(lambda dt: datetime.datetime.utcfromtimestamp(dt/1000))
    df = df.rename(columns={'totalTests': 'Cumulative total'})
    df['Cumulative total'] = df['Cumulative total'].astype(int)
    # KLUDGE: there are a few days with two reports on the same day (but at 
    # different times, like 10am vs 10pm). Upon inspection, it appears that the 
    # latter reports (e.g. the 10pm reports) actually correspond to official cumulative
    # totals for the subsequent day (as determined by comparing to official updates
    # published on Twitter and Facebook). So I increment the date of these latter 
    # reports by one.
    df = df.sort_values('reportdt')
    duplicate_idx = df.index[df['reportdt'].dt.date.duplicated(keep='first')]
    # df.loc[df['reportdt'].dt.date.duplicated(keep=False), ['reportdt', 'Cumulative total', 'test24hours']]
    for idx in duplicate_idx:
        df.loc[idx, 'reportdt'] = df.loc[idx, 'reportdt'] + datetime.timedelta(days=1)
    df['Date'] = df['reportdt'].dt.strftime('%Y-%m-%d')
    # df.loc[df['Date'].duplicated(keep=False), ['Date', 'reportdt', 'Cumulative total', 'test24hours']]
    # df.loc[(df['Date'] >= '2020-08-06') & (df['Date'] <= '2020-08-09'), ['Date', 'reportdt', 'Cumulative total', 'test24hours']]
    df = df[['Date', 'Cumulative total']]
    df = df[df["Cumulative total"] > 0]
    df = df.groupby("Cumulative total", as_index=False).min()
    df = df.groupby("Date", as_index=False).min()
    return df

def sanity_checks(df: pd.DataFrame) -> None:
    """checks that there are no obvious errors in the scraped data.
    """
    # checks that there are no duplicate dates
    assert df['Date'].duplicated().sum() == 0, 'One or more rows have a duplicate date.'
    # checks that the cumulative number of tests on date t is always greater than the figure for t-1:
    assert (df['Cumulative total'].iloc[1:] >= df['Cumulative total'].shift(1).iloc[1:]).all(), "On one or more dates, `Cumulative total` is greater on date t-1."
    # cross-checks a sample of scraped figures against the expected result.
    assert len(official_cumulative_totals) > 0
    for dt, d in official_cumulative_totals:
        val = df.loc[df['Date'] == dt, 'Cumulative total'].squeeze().sum()
        assert val == d['cumulative_total'], f"scraped value ({val:,d}) != official value ({d['cumulative_total']:,d}) on {dt}"

if __name__ == '__main__':
    main()
