"""Constructs daily time series of COVID-19 testing data for Libya.

Dashboard: https://ncdc.org.ly/Ar/libyan-covid-19-dashboard/

Notes:

* In the ArcGIS API, the `field_3` variable is defined as "suspected
    cases", but it actually refers to the daily change in the cumulative
    total of samples tested. We are confident about this b/c the figures
    in this field match the testing figures reported in daily situation
    reports on the official NCDC Facebook page
    (https://www.facebook.com/NCDC.LY). Also, the official dashboard
    itself reports a snapshot of the cumulative number of samples
    tested, which is computed as the sum of `field_3`.
"""

import json
import requests
import datetime
import pandas as pd

COUNTRY = 'Libya'
UNITS = 'samples tested'
TESTING_TYPE = 'PCR only'
SOURCE_LABEL = 'Libya National Centre for Disease Control'
SOURCE_URL = 'https://ncdc.org.ly/Ar/libyan-covid-19-dashboard/'

SERIES_TYPE = 'Daily change in cumulative total'  # one of: {'Cumulative total', 'Daily change in cumulative total'}
DATE_COL = 'field_2'
DATA_URL = 'https://services3.arcgis.com/CoIPqtHxRZqBsYyb/arcgis/rest/services/%D9%86%D9%85%D9%88%D8%B0%D8%AC_%D8%A7%D9%84%D9%85%D9%86%D8%AD%D9%86%D9%89_2view2/FeatureServer/0/query'
# DATA_URL = 'https://services3.arcgis.com/CoIPqtHxRZqBsYyb/arcgis/rest/services/FORM2_1/FeatureServer/0/query'
PARAMS = {
    'f': 'json',
    'where': f"{DATE_COL}>'2020-01-01 00:00:00'", # "Dates>'2020-01-01 00:00:00'",
    'returnGeometry': False,
    'spatialRel': 'esriSpatialRelIntersects',
    'outFields': f'{DATE_COL},field_3',
    'orderByFields': f'{DATE_COL} asc',
    'resultOffset': 0,
    'resultRecordCount': 32000,
    'resultType': 'standard'
}

# hardcoded values
hardcoded_data = [
    # {'Date': "", SERIES_TYPE: , "Source URL": ""},
]


# sample of official values for cross-checking against the API data.
sample_official_data = [
    ("2020-09-10", {SERIES_TYPE: 4009, "source": "https://www.facebook.com/NCDC.LY/posts/2748608298743710"}),
    ("2020-09-02", {SERIES_TYPE: 3606, "source": "https://www.facebook.com/NCDC.LY/posts/2741263856144821"}),
    ("2020-08-21", {SERIES_TYPE: 2562, "source": "https://www.facebook.com/NCDC.LY/posts/2729987143939159"}),
    ("2020-08-04", {SERIES_TYPE: 1090, "source": "https://www.facebook.com/NCDC.LY/posts/2714456478825559"}),
    ("2020-08-03", {SERIES_TYPE: 1157, "source": "https://www.facebook.com/NCDC.LY/posts/2713587508912456"}),
    ("2020-08-02", {SERIES_TYPE: 907, "source": "https://www.facebook.com/NCDC.LY/posts/2712686872335853"}),
    ("2020-07-20", {SERIES_TYPE: 1179, "source": "https://www.facebook.com/NCDC.LY/posts/2701249316812942"}),
    ("2020-07-19", {SERIES_TYPE: 914, "source": "https://www.facebook.com/NCDC.LY/posts/2700451506892723"}),
    ("2020-07-06", {SERIES_TYPE: 672, "source": "https://www.facebook.com/NCDC.LY/posts/2689782814626259"}),
    ("2020-06-28", {SERIES_TYPE: 922, "source": "https://www.facebook.com/NCDC.LY/posts/2682945731976634"}),
    ("2020-06-01", {SERIES_TYPE: 658, "source": "https://www.facebook.com/NCDC.LY/posts/2658400061097868"}),
    ("2020-05-31", {SERIES_TYPE: 475, "source": "https://www.facebook.com/NCDC.LY/posts/2657480017856539"}),
    ("2020-05-14", {SERIES_TYPE: 56, "source": "https://www.facebook.com/NCDC.LY/posts/2642081409396400"}),
    ("2020-04-20", {SERIES_TYPE: 58, "source": "https://www.facebook.com/NCDC.LY/posts/2621174044820470"}),
    ("2020-03-21", {SERIES_TYPE: 2, "source": "https://www.facebook.com/NCDC.LY/posts/2591667084437833"}),
    ("2020-03-17", {SERIES_TYPE: 5, "source": "https://www.facebook.com/NCDC.LY/posts/2587502894854252"}),
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
    df.to_csv("automated_sheets/Libya.csv", index=False)
    return None


def get_data() -> pd.DataFrame:
    res = requests.get(DATA_URL, params=PARAMS)
    json_data = json.loads(res.text)
    df = pd.DataFrame([d['attributes'] for d in json_data['features']])
    df[DATE_COL] = df[DATE_COL].astype(int).apply(lambda dt: datetime.datetime.utcfromtimestamp(dt/1000))
    # KLUDGE: rows with a datetime hour after 22h should be assigned to the
    # next day to be consistent with the daily situation reports on the official 
    # NCDC Facebook page (https://www.facebook.com/NCDC.LY).
    df[DATE_COL] = df[DATE_COL].apply(lambda dt: dt + datetime.timedelta(days=1) if dt.hour >= 22 else dt)
    df['Date'] = df[DATE_COL].dt.strftime('%Y-%m-%d')
    # drops duplicate YYYY-MM-DD rows.
    # df[df[DATE_COL].dt.strftime('%Y-%m-%d').duplicated(keep=False)]  # prints out rows with duplicate YYYY-MM-DD value
    df.sort_values(DATE_COL, inplace=True)
    df.drop_duplicates(subset=['Date'], keep='last', inplace=True)
    df.rename(columns={'field_3': 'Daily change in cumulative total'}, inplace=True)
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
    df = df[df["Daily change in cumulative total"] != 0]
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
