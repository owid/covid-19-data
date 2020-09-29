"""Constructs daily time series of COVID-19 testing data for Tunisia.

Dashboard: https://covid-19.tn/fr/tableau-de-bord/

Notes:

* The official source seems to be double-counting the daily tests on
  2020-04-05 b/c there are two rows of "daily change" data in the
  official dataset and the cumulative total jumps by ~1000 between
  2020-04-04 and 2020-04-05 (but every day in the week before/after only
  had ~500 tests). Nevertheless, because this is how the official source
  presents the data (and there is a chance that the additional row
  represents a testing backlog), we do not make any corrections to this
  possible error.

* The "cumulative total" time series is not visible in the official
    dashboard, but it is nevertheless available via the underlying
    ArcGIS API for the dashboard.
"""

import json
import requests
import datetime
import pandas as pd

COUNTRY = 'Tunisia'
UNITS = 'tests performed'
TESTING_TYPE = 'PCR only'
SOURCE_LABEL = 'Tunisian Ministry of Health'
SOURCE_URL = 'https://covid-19.tn/fr/tableau-de-bord/'

SERIES_TYPE = 'Cumulative total'  # one of: {'Cumulative total', 'Daily change in cumulative total'}
DATE_COL = 'Dates'
DATA_URL = 'https://services6.arcgis.com/BiTAc9ApDDtL9okN/arcgis/rest/services/COVID19_Table_DATESetTOTAL/FeatureServer/0/query'
PARAMS = {
    'f': 'json',
    'where': f"{DATE_COL}>'2020-01-01 00:00:00'",
    'returnGeometry': False,
    'spatialRel': 'esriSpatialRelIntersects',
    'outFields': f'{DATE_COL},Nb_test,Nb_tests_journalier',
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
    ("2020-09-06", {SERIES_TYPE: 165423, "source": "https://covid-19.tn/fr/tableau-de-bord/"}),
    ("2020-08-22", {SERIES_TYPE: 129104, "source": "https://covid-19.tn/fr/tableau-de-bord/"}),
    ("2020-08-12", {SERIES_TYPE: 111517, "source": "https://covid-19.tn/fr/tableau-de-bord/"}),
    ("2020-07-22", {SERIES_TYPE: 89318, "source": "https://covid-19.tn/fr/tableau-de-bord/"}),
    ("2020-06-26", {SERIES_TYPE: 69219, "source": "https://covid-19.tn/fr/tableau-de-bord/"}),
    ("2020-06-01", {SERIES_TYPE: 53539, "source": "https://covid-19.tn/fr/tableau-de-bord/"}),
    ("2020-05-04", {SERIES_TYPE: 25555, "source": "https://covid-19.tn/fr/tableau-de-bord/"}),
    ("2020-03-11", {SERIES_TYPE: 178, "source": "https://covid-19.tn/fr/tableau-de-bord/"}),
    ("2020-03-10", {SERIES_TYPE: 150, "source": "https://covid-19.tn/fr/tableau-de-bord/"}),
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
    df.to_csv("automated_sheets/Tunisia.csv", index=False)
    return None


def get_data() -> pd.DataFrame:
    res = requests.get(DATA_URL, params=PARAMS)
    json_data = json.loads(res.text)
    df = pd.DataFrame([d['attributes'] for d in json_data['features']])
    df[DATE_COL] = df[DATE_COL].astype(int).apply(lambda dt: datetime.datetime.utcfromtimestamp(dt/1000))
    df['Date'] = df[DATE_COL].dt.strftime('%Y-%m-%d')
    # drops duplicate YYYY-MM-DD rows.
    # df[df[DATE_COL].dt.strftime('%Y-%m-%d').duplicated(keep=False)]  # prints out rows with duplicate YYYY-MM-DD value
    df.sort_values(DATE_COL, inplace=True)
    df.drop_duplicates(subset=['Date'], keep='last', inplace=True)
    df.rename(columns={'Nb_tests_journalier': 'Daily change in cumulative total', 'Nb_test': 'Cumulative total'}, inplace=True)
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
    assert len(sample_official_data) > 0
    for dt, d in sample_official_data:
        val = df_temp.loc[df_temp['Date'] == dt, SERIES_TYPE].squeeze().sum()
        assert val == d[SERIES_TYPE], f"scraped value ({val:,d}) != official value ({d[SERIES_TYPE]:,d}) on {dt}"
    return None


if __name__ == '__main__':
    main()
