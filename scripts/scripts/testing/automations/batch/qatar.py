"""Constructs daily time series of COVID-19 testing data for Qatar.

Dashboard: https://www.data.gov.qa/pages/dashboard-covid-19-cases-in-qatar/

"""

import json
import requests
import datetime
import pandas as pd

COUNTRY = 'Qatar'
UNITS = 'people tested'
TESTING_TYPE = 'PCR only'
SOURCE_LABEL = 'Qatar Ministry of Public Health'
SOURCE_URL = 'https://www.data.gov.qa/pages/dashboard-covid-19-cases-in-qatar/'

SERIES_TYPE = 'Cumulative total'  # one of: {'Cumulative total', 'Daily change in cumulative total'}
DATE_COL = 'date'
DATA_URL = 'https://www.data.gov.qa/api/records/1.0/search'
PARAMS = {
    'sort': 'date',
    'rows': 10000,
    'dataset': 'covid-19-cases-in-qatar',
    'fields': f'{DATE_COL},total_number_of_tests_to_date,number_of_new_tests_in_last_24_hrs'
}

# hardcoded values
hardcoded_data = [
    # {'Date': "", SERIES_TYPE: , "Source URL": ""},
]


# sample of official values for cross-checking against the unofficial data.
sample_official_data = [
    ("2020-03-14", {SERIES_TYPE: 6788, "source": "http://web.archive.org/web/20200314165537/https://www.moph.gov.qa/english/Pages/Coronavirus2019.aspx"}),
    ("2020-03-18", {SERIES_TYPE: 8873, "source": "http://web.archive.org/web/20200319122812/https://www.moph.gov.qa/english/Pages/Coronavirus2019.aspx"}),
    ("2020-04-27", {SERIES_TYPE: 85709, "source": "http://web.archive.org/web/20200427153616/https://www.moph.gov.qa/english/Pages/Coronavirus2019.aspx"}),
    ("2020-04-05", {SERIES_TYPE: 35757, "source": "https://www.moph.gov.qa/english/Pages/Coronavirus2019.aspx"}),
    ("2020-05-19", {SERIES_TYPE: 166182, "source": "https://www.moph.gov.qa/english/Pages/Coronavirus2019.aspx"}),
    ("2020-06-11", {SERIES_TYPE: 274793, "source": "https://www.moph.gov.qa/english/Pages/Coronavirus2019.aspx"}),
    ("2020-06-24", {SERIES_TYPE: 333172, "source": "https://www.moph.gov.qa/english/Pages/Coronavirus2019.aspx"}),
    ("2020-07-27", {SERIES_TYPE: 477194, "source": "https://www.moph.gov.qa/english/Pages/Coronavirus2019.aspx"}),
    ("2020-08-23", {SERIES_TYPE: 589920, "source": "https://www.moph.gov.qa/english/Pages/Coronavirus2019.aspx"}),
    ("2020-09-09", {SERIES_TYPE: 672099, "source": "https://www.moph.gov.qa/english/Pages/Coronavirus2019.aspx"}),
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
    df = df.sort_values("Date").groupby("Cumulative total", as_index=False).head(1)
    df = df[['Country', 'Units', 'Testing type', 'Date', SERIES_TYPE, 'Source URL', 'Source label', 'Notes']]
    df.to_csv(f"automated_sheets/{COUNTRY}.csv", index=False)
    return None


def get_data() -> pd.DataFrame:
    res = requests.get(DATA_URL, params=PARAMS)
    json_data = json.loads(res.text)
    df = pd.DataFrame([d['fields'] for d in json_data['records']])
    assert df.shape[0] < PARAMS['rows'], ('Number of records in dataset exceeds number of records requested. '
                                          'You will need to make multiple API requests to retrieve all data.')
    # drops duplicate YYYY-MM-DD rows.
    # df[df[DATE_COL].duplicated(keep=False)]  # prints out rows with duplicate YYYY-MM-DD value
    # df.sort_values(DATE_COL, inplace=True)
    # df.drop_duplicates(subset=['Date'], keep='last', inplace=True)
    df.rename(columns={'number_of_new_tests_in_last_24_hrs': 'Daily change in cumulative total',
                       'total_number_of_tests_to_date': 'Cumulative total', 
                       DATE_COL: 'Date'}, inplace=True)
    df = df.groupby("Date", as_index=False).min()
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
    # removes rows where cumulative total is 0.
    df = df[df['Cumulative total'] > 0]
    df[SERIES_TYPE] = df[SERIES_TYPE].astype(int)
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.max_colwidth', None):
    #     print(df)

    # Temporary fix for 2020-10-25 and 2020-11-21 (typo in total number of tests)
    df = df[-(df["Date"].isin(["2020-10-25", "2020-11-21"]))]
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
