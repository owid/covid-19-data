"""Constructs daily time series of COVID-19 testing data for Ireland.

Dashboard: https://covid19ireland-geohive.hub.arcgis.com/pages/hospitals-icu--testing

"""

import json
import requests
import datetime
import pandas as pd

COUNTRY = 'Ireland'
UNITS = 'tests performed'
TESTING_TYPE = 'PCR only'
SOURCE_LABEL = 'Government of Ireland'
SOURCE_URL = 'https://covid19ireland-geohive.hub.arcgis.com/pages/hospitals-icu--testing'

SERIES_TYPE = 'Cumulative total'  # one of: {'Cumulative total', 'Daily change in cumulative total'}
DATE_COL = 'Date_HPSC'
DATA_URL = 'https://services-eu1.arcgis.com/z6bHNio59iTqqSUY/arcgis/rest/services/LaboratoryLocalTimeSeriesHistoricView/FeatureServer/0/query'
PARAMS = {
    'f': 'json',
    'where': f"{DATE_COL}>'2020-01-01 00:00:00'", # "Dates>'2020-01-01 00:00:00'",
    'returnGeometry': False,
    'spatialRel': 'esriSpatialRelIntersects',
    'outFields': f'{DATE_COL},TotalLabs,Test24',
    'orderByFields': f'{DATE_COL} asc',
    'resultOffset': 0,
    'resultRecordCount': 32000,
    'resultType': 'standard'
}

# hardcoded values
hardcoded_data = [
    # {'Date': "", SERIES_TYPE: , "Source URL": ""},
]


# sample of official values for cross-checking against the unofficial data.
sample_official_data = [
    ("2020-03-18", {SERIES_TYPE: 6457, "source": "https://covid19ireland-geohive.hub.arcgis.com/pages/hospitals-icu--testing"}),
    ("2020-03-21", {SERIES_TYPE: 10436, "source": "https://covid19ireland-geohive.hub.arcgis.com/pages/hospitals-icu--testing"}),
    ("2020-04-09", {SERIES_TYPE: 58506, "source": "https://covid19ireland-geohive.hub.arcgis.com/pages/hospitals-icu--testing"}),
    ("2020-04-30", {SERIES_TYPE: 169646, "source": "https://covid19ireland-geohive.hub.arcgis.com/pages/hospitals-icu--testing"}),
    ("2020-05-16", {SERIES_TYPE: 282185, "source": "https://covid19ireland-geohive.hub.arcgis.com/pages/hospitals-icu--testing"})
]


def main() -> None:
    df = get_data()
    df['Source URL'] = df['Source URL'].apply(lambda x: SOURCE_URL if pd.isnull(x) else x)
    df['Country'] = COUNTRY
    df['Units'] = UNITS
    df['Testing type'] = TESTING_TYPE
    df['Source label'] = SOURCE_LABEL
    df['Notes'] = ""
    sanity_checks(df)
    df = df[['Country', 'Units', 'Testing type', 'Date', SERIES_TYPE, 'Source URL', 'Source label', 'Notes']]
    df.to_csv("automated_sheets/Ireland.csv", index=False)
    return None


def get_data() -> pd.DataFrame:
    res = requests.get(DATA_URL, params=PARAMS)
    json_data = json.loads(res.text)
    df = pd.DataFrame([d['attributes'] for d in json_data['features']])
    df[DATE_COL] = df[DATE_COL].astype(int).apply(lambda dt: datetime.datetime.utcfromtimestamp(dt/1000))
    df['Date'] = df[DATE_COL].dt.strftime('%Y-%m-%d')
    # drops duplicate YYYY-MM-DD rows.
    # df[df[DATE_COL].dt.strftime('%Y-%m-%d').duplicated(keep=False)]  # prints out rows with duplicate YYYY-MM-DD value
    # df.sort_values(DATE_COL, inplace=True)
    # df.drop_duplicates(subset=['Date'], keep='last', inplace=True)
    df.rename(columns={'Test24': 'Daily change in cumulative total', 'TotalLabs': 'Cumulative total'}, inplace=True)
    df['Source URL'] = None
    df = df[['Date', SERIES_TYPE, 'Source URL']]
    if len(hardcoded_data) > 0:
        # removes rows from df that are hardcoded
        hardcoded_dates = [d['Date'] for d in hardcoded_data]
        df = df[~df['Date'].isin(hardcoded_dates)]
        # appends hardcoded rows to df
        df_hardcoded = pd.DataFrame(hardcoded_data)
        df = pd.concat([df, df_hardcoded], axis=0, sort=False).reset_index(drop=True)
    df.sort_values('Date', inplace=True)
    df.dropna(subset=['Date', SERIES_TYPE], how='any', inplace=True)
    df[SERIES_TYPE] = df[SERIES_TYPE].astype(int)
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.max_colwidth', None):
    #     print(df)
    return df


def sanity_checks(df: pd.DataFrame) -> None:
    """checks that there are no obvious errors in the scraped data.
    """
    df_temp = df.copy()
    # checks that the max date is less than tomorrow's date.
    assert datetime.datetime.strptime(df_temp['Date'].max(), '%Y-%m-%d') < (datetime.datetime.utcnow() + datetime.timedelta(days=1))
    # checks that there are no duplicate dates
    assert df_temp['Date'].duplicated().sum() == 0, 'One or more rows share the same date.'
    if 'Cumulative total' not in df_temp.columns:
        df_temp['Cumulative total'] = df_temp['Daily change in cumulative total'].cumsum()
    # checks that the cumulative number of tests on date t is always greater than the figure for t-1:
    assert (df_temp['Cumulative total'].iloc[1:] >= df_temp['Cumulative total'].shift(1).iloc[1:]).all(), "On one or more dates, `Cumulative total` is greater on date t-1."
    # df.iloc[1:][df['Cumulative total'].iloc[1:] < df['Cumulative total'].shift(1).iloc[1:]]
    # cross-checks a sample of scraped figures against the expected result.
    # assert len(sample_official_data) > 0
    # for dt, d in sample_official_data:
    #     val = df_temp.loc[df_temp['Date'] == dt, SERIES_TYPE].squeeze().sum()
    #     assert val == d[SERIES_TYPE], f"scraped value ({val:,d}) != official value ({d[SERIES_TYPE]:,d}) on {dt}"
    return None


if __name__ == '__main__':
    main()
