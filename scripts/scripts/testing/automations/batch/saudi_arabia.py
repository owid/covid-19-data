"""Constructs daily time series of COVID-19 testing data for Saudi Arabia.

Dashboard: https://covid19.moh.gov.sa/

"""

import json
import requests
import datetime
import pandas as pd

COUNTRY = 'Saudi Arabia'
UNITS = 'tests performed'
TESTING_TYPE = 'PCR only'
SOURCE_LABEL = 'Ministry of Health'
SOURCE_URL = 'https://covid19.moh.gov.sa/'

SERIES_TYPE = 'Daily change in cumulative total'  # one of: {'Cumulative total', 'Daily change in cumulative total'}
DATE_COL = 'ReportDate'
DATA_URL = 'https://services6.arcgis.com/bKYAIlQgwHslVRaK/arcgis/rest/services/DailyTestPerformance_ViewLayer/FeatureServer/0/query'
PARAMS = {
    'f': 'json',
    'where': f"{DATE_COL}>'2020-01-01 00:00:00'", # "Dates>'2020-01-01 00:00:00'",
    'returnGeometry': False,
    'spatialRel': 'esriSpatialRelIntersects',
    'outFields': f'{DATE_COL},CumulativeTest,DailyTest',
    'orderByFields': f'{DATE_COL} asc',
    'resultOffset': 0,
    'resultRecordCount': 32000,
    'resultType': 'standard'
}

# hardcoded values
hardcoded_data = [
    # {'Date': "", SERIES_TYPE: , "Source URL": ""},
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
    df.to_csv(f"automated_sheets/{COUNTRY}.csv", index=False)
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
    df.rename(columns={'DailyTest': 'Daily change in cumulative total', 'CumulativeTest': 'Cumulative total'}, inplace=True)
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
    return None


if __name__ == '__main__':
    main()
