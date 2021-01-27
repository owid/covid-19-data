"""Constructs daily time series of COVID-19 testing data for Kazakhstan.

Dashboard: https://hls.kz/

Notes:

* This module requires ChromeDriver to be installed
    (https://chromedriver.chromium.org/downloads) and in your executable
    $PATH.
"""

import time
import json
import datetime
import pandas as pd
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options  


COUNTRY = 'Kazakhstan'
UNITS = 'tests performed'
TESTING_TYPE = 'unclear'
SOURCE_LABEL = 'Kazakhstan National Center for Public Health'
SOURCE_URL = 'https://hls.kz/'

SERIES_TYPE = 'Cumulative total'  # one of: {'Cumulative total', 'Daily change in cumulative total'}
URL = "https://qap.datanomix.pro/single/?appid=9ad0ce19-79e5-4c5a-9fc0-83f858de0153&sheet=dfd9c555-2dfa-4ce9-8231-5d588004e1ef"
COMPONENT_ID = '16a9b11c-56fe-4b07-8598-bcb20677924d'
IMPLICIT_WAIT = 30
MAX_TRIES = 5
TIMEOUT = 120


# hardcoded values
hardcoded_data = [
    # {'Date': "", SERIES_TYPE: , "Source URL": ""},
]


# sample of official values for cross-checking against the API data.
sample_official_data = [
    ("2020-09-04", {SERIES_TYPE: 2571562, "source": "https://hls.kz/"}),
    ("2020-08-12", {SERIES_TYPE: 2252153, "source": "https://hls.kz/"}),
    ("2020-07-01", {SERIES_TYPE: 1536607, "source": "https://hls.kz/"}),
    ("2020-06-20", {SERIES_TYPE: 1302094, "source": "https://hls.kz/"}),
    ("2020-04-30", {SERIES_TYPE: 249527, "source": "https://hls.kz/"}),
    ("2020-04-04", {SERIES_TYPE: 45552, "source": "https://hls.kz/"}),
    ("2020-03-14", {SERIES_TYPE: 445, "source": "https://hls.kz/"}),
    ("2020-03-13", {SERIES_TYPE: 126, "source": "https://hls.kz/"}),
]


def main() -> None:
    i = 0
    df = None
    while df is None and i < MAX_TRIES:
        #print(f'retrieving COVID-19 testing data (attempt {1+i} of {MAX_TRIES})...')
        df = get_data()
        i += 1
    assert df is not None, f'Failed to retrieve testing data after {i} tries.'
    df['Source URL'] = df['Source URL'].apply(lambda x: SOURCE_URL if pd.isnull(x) else x)
    df['Country'] = COUNTRY
    df['Units'] = UNITS
    df['Testing type'] = TESTING_TYPE
    df['Source label'] = SOURCE_LABEL
    df['Notes'] = ""
    sanity_checks(df)
    df = df[['Country', 'Units', 'Testing type', 'Date', SERIES_TYPE, 'Source URL', 'Source label', 'Notes']]
    df.to_csv("automated_sheets/Kazakhstan.csv", index=False)
    return None


def get_data() -> pd.DataFrame:
    options = Options()
    options.add_argument("--headless")
    caps = DesiredCapabilities.CHROME
    caps['goog:loggingPrefs'] = {'performance': 'ALL'}
    try:
        driver = webdriver.Chrome(desired_capabilities=caps, options=options)
        driver.implicitly_wait(IMPLICIT_WAIT)
        driver.get(URL)
        # retrieves browser logs.
        wait = 5
        time.sleep(wait*5)  # the dashboard tends to be slow to load.
        t = wait*3
        browser_log = []
        n_new_logs = 0
        while (len(browser_log) < 1000 or n_new_logs > 0) and t < TIMEOUT:
            new_logs = driver.get_log('performance')
            n_new_logs = len(new_logs)
            if n_new_logs > 0:
                browser_log += new_logs
            time.sleep(wait)
            t += wait
        assert len(browser_log) > 1000, (f'Found only {len(browser_log)} browser '
                                          'log events, but expected > 1000 events. '
                                          'If this problem persists, check that '
                                         f'the dashboard is functional ({URL}) '
                                          'and try increasing '
                                          'time.sleep().')
        # subsets browser logs to websocket responses received and then
        # finds websocket response containing the testing time series.
        events = [json.loads(entry['message'])['message'] for entry in browser_log]
        ws_events_recv = [e for e in events if e['method'] == 'Network.webSocketFrameReceived']
        found_testing_data = False
        while not found_testing_data and ws_events_recv:
            e = ws_events_recv.pop(0)
            resp_data = json.loads(e['params']['response']['payloadData'])
            try:
                data_matrix = resp_data['result']['qLayout'][0]['value']['qHyperCube']['qDataPages'][0]['qMatrix']
                component_id = resp_data['result']['qLayout'][0]['value']['qInfo']['qId']
                if component_id == COMPONENT_ID:    
                    found_testing_data = True
            except:
                pass
        assert found_testing_data, ('Failed to find testing data in websocket '
                                    'responses. If this problem persists, check '
                                    f'that the dashboard is functional ({URL}) '
                                    'and try increasing time.sleep().')
        df = []
        for l in data_matrix:
            row = {'Date': l[0]['qText'],
                'Cumulative total': l[1]['qNum'],
                'Daily change in cumulative total': l[2]['qNum']}
            df.append(row)
        df = pd.DataFrame(df)
        df['Date'] = pd.to_datetime(df['Date'], format='%d.%m.%Y').dt.strftime('%Y-%m-%d')
        df['Cumulative total'] = df['Cumulative total'].astype(float)
        df['Daily change in cumulative total'] = df['Daily change in cumulative total'].astype(float)
        df = df[(df['Daily change in cumulative total'] > 0) | df['Daily change in cumulative total'].isnull()]
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
    except Exception as e:
        df = None
        print(f'Error in retrieving testing data: {e}')
    finally:
        driver.quit()
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
