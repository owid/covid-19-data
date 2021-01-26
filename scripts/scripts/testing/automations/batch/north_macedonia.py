"""Constructs daily time series of COVID-19 testing data for North Macedonia.

Official dashboard: https://koronavirus.gov.mk/stat
(https://datastudio.google.com/embed/u/0/reporting/9f5104d0-12fd-4e16-9a11-993685cfd40f/page/1M)

Notes:

    * This script relies on the possibly tenuous assumption that the variable
        code for the "cumulative tests" ("qt_ievmzlz97b") will not change over time.
        If it does, then the script will either raise an error or return a time
        series that is not the cumulative number of tests.

"""

import re
import json
import datetime
import time
import pandas as pd
from typing import List
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options


COUNTRY = 'North Macedonia'
UNITS = 'tests performed'
TESTING_TYPE = 'unclear'
SOURCE_LABEL = 'Ministry of Health'
SOURCE_URL = 'https://koronavirus.gov.mk/stat'
DASHBOARD_URL = 'https://datastudio.google.com/embed/u/0/reporting/9f5104d0-12fd-4e16-9a11-993685cfd40f/page/1M'
SERIES_TYPE = 'Cumulative total'  # one of: {'Cumulative total', 'Daily change in cumulative total'}
IMPLICIT_WAIT = 15
MAX_TRIES = 5
CUMULATIVE_TESTS_CODE = "qt_ievmzlz97b"  # dashboard column code for the "cumulative tests" variable.


# hardcoded values (for observations in which scraping fails)
hardcoded_data = [
    # {'Date': "", "Cumulative total": , "Source URL": ""},
]

# sample of official values for cross-checking against scraped data.
sample_official_data = [
    ("2020-08-25", {SERIES_TYPE: 140313, "source": "https://datastudio.google.com/embed/u/0/reporting/9f5104d0-12fd-4e16-9a11-993685cfd40f/page/1M"}),
    ("2020-08-23", {SERIES_TYPE: 136480, "source": "https://datastudio.google.com/embed/u/0/reporting/9f5104d0-12fd-4e16-9a11-993685cfd40f/page/1M"}),
    ("2020-07-20", {SERIES_TYPE: 86544, "source": "https://datastudio.google.com/embed/u/0/reporting/9f5104d0-12fd-4e16-9a11-993685cfd40f/page/1M"}),
    ("2020-06-03", {SERIES_TYPE: 32161, "source": "https://datastudio.google.com/embed/u/0/reporting/9f5104d0-12fd-4e16-9a11-993685cfd40f/page/1M"}),
    ("2020-04-23", {SERIES_TYPE: 13649, "source": "https://datastudio.google.com/embed/u/0/reporting/9f5104d0-12fd-4e16-9a11-993685cfd40f/page/1M"}),
    ("2020-03-30", {SERIES_TYPE: 3126, "source": "https://datastudio.google.com/embed/u/0/reporting/9f5104d0-12fd-4e16-9a11-993685cfd40f/page/1M"}),
]


def main() -> None:
    df = get_data()
    df.loc[:, 'Country'] = COUNTRY
    df.loc[:, 'Units'] = UNITS
    df.loc[:, 'Testing type'] = TESTING_TYPE
    df.loc[:, 'Source label'] = SOURCE_LABEL
    df.loc[:, 'Notes'] = ""
    sanity_checks(df)
    df = df[['Country', 'Units', 'Testing type', 'Date', SERIES_TYPE, 'Source URL', 'Source label', 'Notes']]
    df.to_csv("automated_sheets/North Macedonia.csv", index=False)
    return None


def get_data() -> List[str]:
    i = 0
    json_data = None
    while json_data is None and i < MAX_TRIES:
        #print(f'Retrieving testing data (attempt {1+i} of {MAX_TRIES})...')
        json_data = _get_batched_data_json()
        i += 1
    assert json_data is not None, f'Failed to retrieve testing data after {i} tries.'
    #print('Constructing and cleaning dataframe...')
    df = _construct_df(json_data)
    df = _clean_df(df)
    return df


def _get_batched_data_json() -> List[dict]:
    """retrieves a list of plotly traces in json format, which contain the time
    series of daily tests.
    
    Returns:

        plotly_data: List[dict]. A list of plotly traces in json format.
    """
    try:
        options = Options()
        options.add_argument("--headless") 
        seleniumwire_options = {'connection_timeout': None}
        driver = webdriver.Chrome(options=options, seleniumwire_options=seleniumwire_options)
        driver.implicitly_wait(IMPLICIT_WAIT)
        # in this instance, we only want to monitor requests that contain "batchedDataV2"
        # in the url
        driver.scopes = ['.*batchedDataV2.*']
        driver.get(DASHBOARD_URL)
        # wait a few seconds to ensure that the xhr response with the
        # testing time series data is received.
        time.sleep(5)
        # finds the xhr request that contains the testing time series data.
        found_testing_data_response = False
        i = 0
        while not found_testing_data_response and i < len(driver.requests):
            request = driver.requests[i]
            if request.response and 'batchedDataV2' in request.url:
                try:
                    body = json.loads(request.body)
                    for data_request in body['dataRequest']:
                        for query_field in data_request['datasetSpec']['queryFields']:
                            if query_field['name'] == CUMULATIVE_TESTS_CODE:
                                found_testing_data_response = True
                                break
                except:
                    pass
            i += 1
            if i == len(driver.requests):
                # wait a few seconds in case more requests are pending.
                time.sleep(5)
        assert found_testing_data_response, 'failed to find XHR request containing the testing time series data.'
        # extracts json response
        regex = re.search(rb'([\)\]\}\',]{1,10})\n(.*)', request.response.body)
        json_data = json.loads(regex.group(2))
    except Exception as e:
        json_data = None
        print(f'Error in retrieving json data from Data Studio dashboard: {e}')
    finally:
        driver.quit()
    return json_data


def _construct_df(json_data: dict) -> dict:
    assert 'default' in json_data
    data_json = None
    i = 0
    while data_json is None and i < len(json_data['default']['dataResponse']):
        data_response = json_data['default']['dataResponse'][i]
        for data_response in json_data['default']['dataResponse']:
            for data_subset in data_response['dataSubset']:
                column_codes = [column_info['name'] for column_info in data_subset['dataset']['tableDataset']['columnInfo']]
                if CUMULATIVE_TESTS_CODE in column_codes:
                    data_json = data_subset['dataset']['tableDataset']['column']
                    break
        i += 1
    assert len(data_json) == 2, 'Expected only two data columns -- retrieved data is probably not the "cumulative tests" time series you are expecting'
    return pd.DataFrame({
        'Date': data_json[0]['stringColumn']['values'],
        'Cumulative total': data_json[1]['doubleColumn']['values'],
    })


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, 'Date'] = pd.to_datetime(df['Date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
    df.loc[:, 'Cumulative total'] = df['Cumulative total'].astype(int)
    df = df[df['Cumulative total'] > 0].copy()
    df = df.groupby("Cumulative total", as_index=False).min()
    df.loc[:, 'Source URL'] = SOURCE_URL
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
    df.loc[:, SERIES_TYPE] = df[SERIES_TYPE].astype(int)
    return df


def sanity_checks(df: pd.DataFrame) -> None:
    """checks that there are no obvious errors in the scraped data."""
    df_temp = df.copy()
    # checks that the max date is less than or equal to tomorrow's date.
    assert datetime.datetime.strptime(df_temp['Date'].max(), '%Y-%m-%d') < (datetime.datetime.utcnow() + datetime.timedelta(days=1))
    # checks that there are no duplicate dates
    assert df_temp['Date'].duplicated().sum() == 0, 'One or more rows share the same date.'
    if 'Cumulative total' not in df_temp.columns:
        df_temp.loc[:, 'Cumulative total'] = df_temp['Daily change in cumulative total'].cumsum()
    # checks that the cumulative number of tests on date t is always greater than the figure for t-1:
    assert (df_temp['Cumulative total'].iloc[1:] >= df_temp['Cumulative total'].shift(1).iloc[1:]).all(), "On one or more dates, `Cumulative total` is greater on date t-1."
    # cross-checks a sample of scraped figures against the expected result.
    assert len(sample_official_data) > 0
    for dt, d in sample_official_data:
        val = df_temp.loc[df['Date'] == dt, SERIES_TYPE].astype(int).squeeze().sum()
        assert val == d[SERIES_TYPE], f"scraped value ({val:,d}) != official value ({d[SERIES_TYPE]:,d}) on {dt}"
    return None


if __name__ == '__main__':
    main()
