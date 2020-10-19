"""Constructs daily time series of COVID-19 testing data for Guatemala.

Official dashboard: https://tablerocovid.mspas.gob.gt/.

This module extracts the "Casos tamizados por resultado y por fecha de
emisión de resultados" time series, which is displayed in a bar chart
after clicking on the "Casos tamizados" tab on the left-hand side of the
dashboard. The time series displays the number of daily negative and
positive tests, which this module combines to arrive at the number of
people tested each day.
"""

import re
import json
import datetime
import time
import pandas as pd
from typing import List
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options


COUNTRY = 'Guatemala'
UNITS = 'people tested'
TESTING_TYPE = 'includes non-PCR'
SOURCE_LABEL = 'Ministry of Health and Social Assistance'
SOURCE_URL = 'https://tablerocovid.mspas.gob.gt/'
DASHBOARD_URL = 'https://gtmvigilanciacovid.shinyapps.io/3869aac0fb95d6baf2c80f19f2da5f98/'
SERIES_TYPE = 'Daily change in cumulative total'  # one of: {'Cumulative total', 'Daily change in cumulative total'}
IMPLICIT_WAIT = 15
MAX_TRIES = 5


def main() -> None:
    df = get_data()
    df['Country'] = COUNTRY
    df['Units'] = UNITS
    df['Testing type'] = TESTING_TYPE
    df['Source label'] = SOURCE_LABEL
    df['Notes'] = ""
    sanity_checks(df)
    df = df[['Country', 'Units', 'Testing type', 'Date', SERIES_TYPE, 'Source URL', 'Source label', 'Notes']]
    df.to_csv("automated_sheets/Guatemala.csv", index=False)
    return None


def get_data() -> List[str]:
    i = 0
    plotly_traces = None
    while plotly_traces is None and i < MAX_TRIES:
        # print(f'Retrieving testing data (attempt {1+i} of {MAX_TRIES})...')
        plotly_traces = _get_plotly_traces()
        i += 1
    assert plotly_traces is not None, f'Failed to retrieve testing data after {i} tries.'
    #print('Constructing and cleaning dataframe...')
    df = _construct_df(plotly_traces)
    df = _clean_df(df)
    return df


def _get_plotly_traces() -> List[dict]:
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
        # in this instance, we only want to monitor requests that end in "xhr"
        # i.e. https://gtmvigilanciacovid.shinyapps.io/.../xhr
        driver.scopes = ['.*xhr$']
        driver.get(DASHBOARD_URL)
        driver.find_element_by_xpath('//a[@href="#shiny-tab-sospechososTab"]').click()
        # wait a few seconds to ensure that the xhr response with the
        # testing time series data is received.
        time.sleep(5)
        # finds the xhr request that contains the testing time series data.
        found_testing_data = False
        i = 0
        while not found_testing_data and i < len(driver.requests):
            request = driver.requests[i]
            if request.response and b'proporcionSospechososFecha' in request.response.body:
                found_testing_data = True
            i += 1
            if i == len(driver.requests):
                # wait a few seconds in case more requests are pending.
                time.sleep(5)
        # assert found_testing_data, 'failed to find XHR request containing the testing time series data.'
        regex = re.search(rb'.*(\[\".+\"\])', request.response.body)
        json_data = json.loads(regex.groups()[0])
        # within the response body, we want to find a specific dict that contains
        # the testing time series data...
        found_time_series_dict = False
        i = 0
        while not found_time_series_dict and i < len(json_data):
            dict_str = json_data[i]
            if 'proporcionSospechososFecha' in dict_str and len(dict_str) > 1000:
                found_time_series_dict = True
            i += 1
        # assert found_time_series_dict, 'failed to find the testing time series data in the XHR request.'
        regex = re.search(r'.{1,10}(\{.+\})', dict_str)
        d = json.loads(regex.groups()[0])
        plotly_traces = d['values']['proporcionSospechososFecha']['x']['data']
    except Exception as e:
        plotly_traces = None
        # print(f'Error in retrieving testing data json: {e}')
    finally:
        driver.quit()
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.max_colwidth', None, 'display.width', None):
    #     print(df)
    return plotly_traces


def _construct_df(plotly_traces: List[dict]) -> pd.DataFrame:
    """constructs a dataframe containing the daily testing time series
    from the plotly traces."""
    ts_dates0 = plotly_traces[0]['x']
    ts_positive = plotly_traces[0]['y']
    ts_dates1 = plotly_traces[1]['x']
    ts_negative = plotly_traces[1]['y']
    # ts_pct_positive = plotly_traces[2]['y']
    assert (pd.Series(ts_dates0) == pd.Series(ts_dates1)).all(), ('One or more dates in the "positive_today" time series '
                                                                  'does not match the date in the "negative_today" time '
                                                                  'series.')
    df = pd.DataFrame({'Date': ts_dates0, 'positive_today': ts_positive, 'negative_today': ts_negative})
    df['positive_today'] = df['positive_today'].astype(int)
    df['negative_today'] = df['negative_today'].astype(int)
    df['tests_today'] = df['positive_today'] + df['negative_today']
    return df

def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df.rename(columns={'tests_today': 'Daily change in cumulative total'}, inplace=True)
    df.loc[:, 'Source URL'] = SOURCE_URL
    df = (
        df[['Date', SERIES_TYPE, 'Source URL']]
        .sort_values('Date')
        .dropna(subset=['Date', SERIES_TYPE], how='any')
    )
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
        df_temp['Cumulative total'] = df_temp['Daily change in cumulative total'].cumsum()
    # checks that the cumulative number of tests on date t is always greater than the figure for t-1:
    assert (df_temp['Cumulative total'].iloc[1:] >= df_temp['Cumulative total'].shift(1).iloc[1:]).all(), "On one or more dates, `Cumulative total` is greater on date t-1."
    return None


if __name__ == '__main__':
    main()
