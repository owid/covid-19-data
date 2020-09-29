"""Constructs daily time series of COVID-19 testing data for the UAE.

Dashboard: https://fcsa.gov.ae/en-us/Pages/Covid19/UAE-Covid-19-Updates.aspx
"""

import json
import requests
import datetime
import pandas as pd

COUNTRY = 'United Arab Emirates'
UNITS = 'tests performed'
TESTING_TYPE = 'PCR only'
SOURCE_LABEL = 'UAE Federal Competitiveness and Statistics Authority'
SOURCE_URL = 'https://fcsa.gov.ae/en-us/Pages/Covid19/UAE-Covid-19-Updates.aspx'

SERIES_TYPE = 'Daily change in cumulative total'  # one of: {'Cumulative total', 'Daily change in cumulative total'}
DATE_COL = 'DATE_'
DATA_URL = 'https://geostat.fcsa.gov.ae/gisserver/rest/services/UAE_COVID19_Statistics_Rates_Layer/FeatureServer/0/query'
PARAMS = {
    'f': 'json',
    'where': "1=1",
    'returnGeometry': False,
    'spatialRel': 'esriSpatialRelIntersects',
    'outFields': f'{DATE_COL},TESTS,CUMULATIVE_TESTS',
    'orderByFields': f'{DATE_COL} asc',
}

# hardcoded values
hardcoded_data = [
    # {'Date': "", SERIES_TYPE: , "Source URL": ""},
]


# sample of official values for cross-checking against the unofficial data.
sample_official_data = [
    ("2020-01-29", {SERIES_TYPE: 75, "source": "https://fcsa.gov.ae/en-us/Pages/Covid19/UAE-Covid-19-Updates.aspx"}),
    ("2020-02-08", {SERIES_TYPE: 574, "source": "https://fcsa.gov.ae/en-us/Pages/Covid19/UAE-Covid-19-Updates.aspx"}),
    ("2020-02-28", {SERIES_TYPE: 8606, "source": "https://fcsa.gov.ae/en-us/Pages/Covid19/UAE-Covid-19-Updates.aspx"}),
    ("2020-03-28", {SERIES_TYPE: 8216, "source": "https://fcsa.gov.ae/en-us/Pages/Covid19/UAE-Covid-19-Updates.aspx"}),
    ("2020-05-02", {SERIES_TYPE: 18428, "source": "https://fcsa.gov.ae/en-us/Pages/Covid19/UAE-Covid-19-Updates.aspx"}),
    ("2020-06-01", {SERIES_TYPE: 41762, "source": "https://fcsa.gov.ae/en-us/Pages/Covid19/UAE-Covid-19-Updates.aspx"}),
    ("2020-07-15", {SERIES_TYPE: 48534, "source": "https://fcsa.gov.ae/en-us/Pages/Covid19/UAE-Covid-19-Updates.aspx"}),
    ("2020-08-08", {SERIES_TYPE: 63792, "source": "https://fcsa.gov.ae/en-us/Pages/Covid19/UAE-Covid-19-Updates.aspx"}),
    ("2020-08-21", {SERIES_TYPE: 82191, "source": "https://fcsa.gov.ae/en-us/Pages/Covid19/UAE-Covid-19-Updates.aspx"}),
    ("2020-09-09", {SERIES_TYPE: 85917, "source": "https://fcsa.gov.ae/en-us/Pages/Covid19/UAE-Covid-19-Updates.aspx"}),
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
    df.to_csv("automated_sheets/United Arab Emirates.csv", index=False)
    return None


def get_data() -> pd.DataFrame:
    res = requests.get(DATA_URL, params=PARAMS)
    json_data = json.loads(res.text)
    df = pd.DataFrame([d['attributes'] for d in json_data['features']])
    df[DATE_COL] = df[DATE_COL].astype(int).apply(lambda dt: datetime.datetime.utcfromtimestamp(dt/1000))
    df[DATE_COL] = df[DATE_COL].dt.strftime('%Y-%m-%d')
    df.rename(columns={'TESTS': 'Daily change in cumulative total', 'CUMULATIVE_TESTS': 'Cumulative total', DATE_COL: 'Date'}, inplace=True)
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
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
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
