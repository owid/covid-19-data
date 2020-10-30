"""Constructs daily time series of COVID-19 testing data for Senegal.

Dashboard: http://www.sante.gouv.sn/

"""

import json
import requests
import datetime
import pandas as pd

COUNTRY = 'Senegal'
UNITS = 'tests performed'
TESTING_TYPE = 'PCR only'
SOURCE_LABEL = 'Ministry for Health and Social Action'
SOURCE_URL = 'http://www.sante.gouv.sn/'

SERIES_TYPE = 'Daily change in cumulative total'  # one of: {'Cumulative total', 'Daily change in cumulative total'}
DATE_COL = 'Date'
DATA_URL = 'https://services7.arcgis.com/Z6qiqUaS6ImjYL5S/arcgis/rest/services/tendance_nationale/FeatureServer/0/query'
PARAMS = {
    'f': 'json',
    'where': f"1=1", # "Dates>'2020-01-01 00:00:00'",
    'returnGeometry': False,
    'spatialRel': 'esriSpatialRelIntersects',
    'outFields': f'{DATE_COL},Nombre_de_tests_realises',
    'orderByFields': f'{DATE_COL} asc',
    'resultOffset': 0,
    'resultRecordCount': 32000,
    'resultType': 'standard'
}

# hardcoded values
# NOTE: we hardcode quite a few values for Senegal b/c the press releases
# date back to 28 Feb 2020 but the dashboard data begins on 1 April 2020
# (and the 1 April 2020 dashboard figure is incorrect, so we replace it with
# with press release figure).
hardcoded_data = [
    {"Date": "2020-04-01", SERIES_TYPE: 195, "source": "http://www.sante.gouv.sn/sites/default/files/Communiqu%C3%A9%20N%C2%B031%20du%2001%20avril%202020.pdf"},
    {"Date": "2020-03-31", SERIES_TYPE: 97, "Source URL": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2030%20DU%2031%20MARS%202020.pdf"},
    {"Date": "2020-03-30", SERIES_TYPE: 87, "Source URL": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2029%20DU%2030%20MARS%202020.pdf"},
    {"Date": "2020-03-29", SERIES_TYPE: 151, "Source URL": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2028%20%20DU%2029%20MARS%202020pdf.pdf"},
    {"Date": "2020-03-28", SERIES_TYPE: 98, "Source URL": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2027%20DU%2028%20MARS%202020.pdf"},
    {"Date": "2020-03-27", SERIES_TYPE: 144, "Source URL": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2026%20DU%2027%20MARS%202020pdf.pdf"},
    {"Date": "2020-03-26", SERIES_TYPE: 130, "Source URL": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2025%20%20du%2026%20mars%202020.pdf"},
    {"Date": "2020-03-25", SERIES_TYPE: 142, "Source URL": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2024%20du%2025%20mars%202020.pdf"},
    {"Date": "2020-03-24", SERIES_TYPE: 59, "Source URL": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2023%20du%2024%20mars%202020.pdf"},
    {"Date": "2020-03-23", SERIES_TYPE: 60, "Source URL": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2022%20du%2023%20mars%202020.pdf"},
    {"Date": "2020-03-22", SERIES_TYPE: 85, "Source URL": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2021%20du%2022%20mars%202020.pdf"},
    {"Date": "2020-03-21", SERIES_TYPE: 31, "Source URL": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2020%20du%2021%20mars%202020.pdf"},
    {"Date": "2020-03-20", SERIES_TYPE: 56, "Source URL": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2019%20du%2020%20mars%202020%20.pdf"},
    {"Date": "2020-03-19", SERIES_TYPE: 22, "Source URL": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2018%20du%2019%20mars%202020%20.pdf"},
    {"Date": "2020-03-18", SERIES_TYPE: 27, "Source URL": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2017%20du%2018%20mars%202020pdf.pdf"},
    {"Date": "2020-03-17", SERIES_TYPE: 44, "Source URL": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2016%20du%2017%20mars%202020.pdf"},
    {"Date": "2020-03-16", SERIES_TYPE: 9, "Source URL": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2015%20du%20mars%202020.pdf.pdf "},
    {"Date": "2020-03-15", SERIES_TYPE: 2, "Source URL": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2014%20du%2015%20mars%202020.pdf.pdf"},
    {"Date": "2020-03-14", SERIES_TYPE: 3, "Source URL": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2013.pdf.pdf"},
    {"Date": "2020-03-13", SERIES_TYPE: 12, "Source URL": "https://twitter.com/MinisteredelaS1/status/1238514022523207686"},
    {"Date": "2020-03-12", SERIES_TYPE: 6, "Source URL": "http://www.sante.gouv.sn/sites/default/files/Communiqu%C3%A911covid19.pdf"},
    {"Date": "2020-03-11", SERIES_TYPE: 8, "Source URL": "http://www.sante.gouv.sn/sites/default/files/Coronavirus%20%20Communiqu%C3%A9%20de%20presse%20du%2011%20mars%202020.pdf"},
    {"Date": "2020-03-10", SERIES_TYPE: 1, "Source URL": "http://www.sante.gouv.sn/sites/default/files/Coronavirus%20communiqu%C3%A9%20de%20presse%20N%C2%B09%20du%20mardi%2010%20mars%202020.pdf"},
    {"Date": "2020-03-09", SERIES_TYPE: 6, "Source URL": "http://www.sante.gouv.sn/sites/default/files/Coronavirus%20%20Communiqu%C3%A9%20N%C2%B08%20Point%20de%20situation.pdf"},
    {"Date": "2020-03-08", SERIES_TYPE: 2, "Source URL": "http://www.sante.gouv.sn/sites/default/files/Coronavirus%20%20Communiqu%C3%A9%20N%C2%B07%20Point%20de%20situation.pdf"},
    {"Date": "2020-03-07", SERIES_TYPE: 2, "Source URL": "http://www.sante.gouv.sn/sites/default/files/Communiqu%C3%A9%20N%C2%B06%20sur%20le%20Coronavirus.pdf"},
    {"Date": "2020-03-06", SERIES_TYPE: 2, "Source URL": "http://www.sante.gouv.sn/sites/default/files/Communiqu%C3%A9%20N%C2%B05%20sur%20le%20Cotonavirus.pdf"},
    {"Date": "2020-03-04", SERIES_TYPE: 11, "Source URL": "http://www.sante.gouv.sn/sites/default/files/Communiqu%C3%A9%20de%20presse%20N%C2%B03%20sur%20le%20Coronavirus.pdf"},
    {"Date": "2020-03-03", SERIES_TYPE: 2, "Source URL": "http://www.sante.gouv.sn/sites/default/files/Communiqu%C3%A9%20de%20presse%20N%C2%B02%20sur%20le%20Coronavirus.pdf"},
    {"Date": "2020-02-28", SERIES_TYPE: 1, "Source URL": "http://www.sante.gouv.sn/sites/default/files/comcovid19.pdf"},
]


# sample of official values for cross-checking against the scraped data.
sample_official_data = [
    ("2020-09-18", {SERIES_TYPE: 1154, "source": "http://www.sante.gouv.sn/sites/default/files/Communiqu%C3%A9201_covid19.pdf"}),
    ("2020-08-20", {SERIES_TYPE: 1519, "source": "http://www.sante.gouv.sn/sites/default/files/Communiqu%C3%A9%20172%20du%2020%20aout%202020.pdf"}),
    ("2020-08-09", {SERIES_TYPE: 1656, "source": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%20161.pdf"}),
    ("2020-07-22", {SERIES_TYPE: 1244, "source": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%20143%20DU%2022%20JUILLET%202020.pdf"}),
    ("2020-07-04", {SERIES_TYPE: 959, "source": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%20125%20DU%2004%20JUILLET%202020.pdf"}),
    ("2020-06-26", {SERIES_TYPE: 1057, "source": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%20117%20DU%2026%20JUIN%202020.pdf"}),
    ("2020-06-13", {SERIES_TYPE: 1378, "source": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%20104%20DU%2013%20JUIN%202020.pdf"}),
    ("2020-05-30", {SERIES_TYPE: 1370, "source": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2090%20DU%2030%20MAI%202020.pdf"}),
    ("2020-04-16", {SERIES_TYPE: 435, "source": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2046%20DU%2016AVRIL%202020%20pdf.pdf"}),
    ("2020-04-02", {SERIES_TYPE: 127, "source": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2032%20DU%2002%20AVRIL%202020.pdf"}),
    ("2020-04-01", {SERIES_TYPE: 195, "source": "http://www.sante.gouv.sn/sites/default/files/Communiqu%C3%A9%20N%C2%B031%20du%2001%20avril%202020.pdf"}),
    ("2020-03-27", {SERIES_TYPE: 144, "source": "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2026%20DU%2027%20MARS%202020pdf.pdf"}),
    ("2020-03-13", {SERIES_TYPE: 12, "source": "https://twitter.com/MinisteredelaS1/status/1238514022523207686"}),  # NOTE: it is a little ambiguous about whether the official figure  is 11 or 12 here.
    ("2020-03-12", {SERIES_TYPE: 6, "source": "http://www.sante.gouv.sn/sites/default/files/Communiqu%C3%A911covid19.pdf"}),
    ("2020-03-04", {SERIES_TYPE: 11, "source": "http://www.sante.gouv.sn/sites/default/files/Communiqu%C3%A9%20de%20presse%20N%C2%B03%20sur%20le%20Coronavirus.pdf"}),
    ("2020-03-03", {SERIES_TYPE: 2, "source": "http://www.sante.gouv.sn/Actualites/coronavirus-communiqu%C3%A9-de-presse-n%C2%B02-du-minist%C3%A8re-de-la-sant%C3%A9-et-de-laction-sociale"}),
    ("2020-02-28", {SERIES_TYPE: 1, "source": "http://www.sante.gouv.sn/Actualites/coronavirus-communiqu%C3%A9-de-presse-n%C2%B01-du-minist%C3%A8re-de-la-sant%C3%A9-et-de-laction-sociale"}),   
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
    df.to_csv("automated_sheets/Senegal.csv", index=False)
    return None


def get_data() -> pd.DataFrame:
    res = requests.get(DATA_URL, params=PARAMS)
    json_data = json.loads(res.text)
    df = pd.DataFrame([d['attributes'] for d in json_data['features']])
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
    # drops duplicate YYYY-MM-DD rows.
    # df[df[DATE_COL].duplicated(keep=False)]  # prints out rows with duplicate YYYY-MM-DD value
    # df.sort_values(DATE_COL, inplace=True)
    # df.drop_duplicates(subset=['Date'], keep='last', inplace=True)
    df.rename(columns={'Nombre_de_tests_realises': 'Daily change in cumulative total'}, inplace=True)
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
    df = df[df["Date"] <= str(datetime.date.today())]
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
