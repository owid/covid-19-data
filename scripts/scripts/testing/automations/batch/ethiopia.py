"""Constructs daily time series of COVID-19 testing data for Ethiopia.

Dashboard: https://www.covid19.et/covid-19/Home/DataPresentationByTable

"""

import json
import requests
import datetime
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver

COUNTRY = 'Ethiopia'
UNITS = 'tests performed'
TESTING_TYPE = 'PCR only'
SOURCE_LABEL = 'Information Network Security Agency'
SOURCE_URL = 'https://www.covid19.et/covid-19/Home/DataPresentationByTable'

SERIES_TYPE = 'Cumulative total'  # one of: {'Cumulative total', 'Daily change in cumulative total'}
DATA_URL = 'https://www.covid19.et/covid-19/Home/DataPresentationByTable'
MAX_TRIES = 5

# hardcoded values
hardcoded_data = [
    # {"Date": "", SERIES_TYPE: , "source": ""},
]


# sample of official values for cross-checking against the scraped data.
sample_official_data = [
    ("2020-09-18", {SERIES_TYPE: 1184473, "source": "https://twitter.com/EPHIEthiopia/status/1306992815730307073"}),
    ("2020-09-11", {SERIES_TYPE: 1122659, "source": "https://twitter.com/EPHIEthiopia/status/1304467982630948864"}),
    ("2020-07-19", {SERIES_TYPE: 331266, "source": "https://www.ephi.gov.et/images/novel_coronavirus/EPHI_-PHEOC_COVID-19_Weekly-bulletin_12_English_07202020.pdf"}),
    ("2020-06-13", {SERIES_TYPE: 176504, "source": "https://www.ephi.gov.et/images/novel_coronavirus/confirmed-case-Press-release_June-13-Eng_V2.pdf"}),
    ("2020-05-30", {SERIES_TYPE: 106615, "source": "https://www.ephi.gov.et/images/novel_coronavirus/confirmed-case-Press-release_May-30--Eng_V1.pdf"}),
    ("2020-05-11", {SERIES_TYPE: 36624, "source": "https://www.ephi.gov.et/images/novel_coronavirus/confirmed-case-Press-release_May-11_-ENG-V1-1.pdf"}),
    ("2020-04-28", {SERIES_TYPE: 15668, "source": "https://www.ephi.gov.et/images/novel_coronavirus/confirmed-case-Press-release_April-28_-ENG-V1-1.pdf"}),
    ("2020-04-25", {SERIES_TYPE: 12688, "source": "https://www.ephi.gov.et/images/novel_coronavirus/confirmed-case-Press-release_April-25_-ENG-V5-1.pdf"}),
    ("2020-03-31", {SERIES_TYPE: 1013, "source": "https://www.ephi.gov.et/images/novel_coronavirus/Press-release_March-31_2020_Eng_TA.pdf"}),
    ("2020-03-22", {SERIES_TYPE: 392, "source": "https://twitter.com/lia_tadesse/status/1241610916736663558"}),
]


def main() -> None:
    i = 0
    df = None
    while df is None and i < MAX_TRIES:
        #print(f'retrieving COVID-19 testing data (attempt {1+i} of {MAX_TRIES})...')
        df = get_data()
        i += 1
    assert df is not None, (f'Failed to retrieve testing data after {i} tries. '
                             'If this problem persists, check that the URL '
                            f'is working ({DATA_URL}).')
    df.loc[:, 'Source URL'] = df['Source URL'].apply(lambda x: SOURCE_URL if pd.isnull(x) else x)
    df.loc[:, 'Country'] = COUNTRY
    df.loc[:, 'Units'] = UNITS
    df.loc[:, 'Testing type'] = TESTING_TYPE
    df.loc[:, 'Source label'] = SOURCE_LABEL
    df.loc[:, 'Notes'] = ""
    sanity_checks(df)
    df = df[['Country', 'Units', 'Testing type', 'Date', SERIES_TYPE, 'Source URL', 'Source label', 'Notes']]
    df.to_csv("automated_sheets/Ethiopia.csv", index=False)
    return None


def get_data() -> pd.DataFrame:
    dfs = pd.read_html(DATA_URL, attrs={'id': 'dataTable'})
    assert len(dfs) > 0, f'Failed to find table with id="dataTable" at {DATA_URL}'
    df = dfs[0].copy()
    assert df.shape[0] > 100, ('Expected table to have > 100 rows, but table '
                              f'only has {df.shape[0]} rows. If this problem '
                               'persists, you may need to use selenium to change'
                               'the number of rows that are displayed on the page.')
    df.rename(columns={df.columns[0]: 'Date',
                       df.columns[1]: 'Daily change in cumulative total',
                       df.columns[2]: 'Cumulative total'}, inplace=True)
    df['Date'] = df['Date'].apply(lambda s: datetime.datetime.strptime(s[:10], '%d/%m/%Y')).dt.strftime('%Y-%m-%d')
    # drops duplicate YYYY-MM-DD rows.
    # df[df['Date'].duplicated(keep=False)]  # prints out rows with duplicate YYYY-MM-DD value
    df.sort_values(['Date', SERIES_TYPE], inplace=True)
    df.drop_duplicates(subset=['Date'], keep='last', inplace=True)
    df = df[df['Date'] != '2020-03-18']
    df = df[['Date', SERIES_TYPE]]
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
    df = df[df["Cumulative total"] > 0]
    df = df.groupby("Cumulative total", as_index=False).min()
    df = df.groupby("Date", as_index=False).min()
    df.loc[:, 'Source URL'] = None
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
