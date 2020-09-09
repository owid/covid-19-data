"""Constructs daily time series of COVID-19 testing data for Bangladesh.
Official dashboard: https://covid19.cramstack.com/ (https://datastudio.google.com/embed/reporting/3ba78724-85a8-499d-b872-64deab426994)
"""
import re
import json
import requests
import subprocess
import time
import pandas as pd
from seleniumwire import webdriver
from selenium.webdriver.support.ui import WebDriverWait

COUNTRY = 'Bangladesh'
UNITS = 'samples tested'
TESTING_TYPE = 'PCR only'
SOURCE_LABEL = 'Government of Bangladesh'
SOURCE_URL = 'https://covid19.cramstack.com/'
DASHBOARD_URL = 'https://datastudio.google.com/embed/reporting/3ba78724-85a8-499d-b872-64deab426994'
IMPLICIT_WAIT = 30
MAX_TRIES = 5

# sample of official values for cross-checking against the scraped data.
official_cumulative_totals = [
    ("2020-08-23", {"cumulative_total": 1442656, "source": "https://covid19.cramstack.com/"}),
    ("2020-08-15", {"cumulative_total": 1341648, "source": "https://covid19.cramstack.com/"}),
    ("2020-07-01", {"cumulative_total": 787335, "source": "https://covid19.cramstack.com/"}),
    ("2020-04-29", {"cumulative_total": 59701, "source": "https://covid19.cramstack.com/"}),
    ("2020-03-15", {"cumulative_total": 268, "source": "https://covid19.cramstack.com/"}),
    ("2020-03-04", {"cumulative_total": 108, "source": "https://covid19.cramstack.com/"}),
]

def main() -> None:
    i = 0
    df = None
    while df is None and i < MAX_TRIES:
        #print(f'retrieving testing data (attempt {1+i} of {MAX_TRIES})...')
        df = get_data()
        i += 1
    assert df is not None, f'Failed to retrieve testing data after {i} tries.'
    df.sort_values('Date', inplace=True)
    df['Country'] = COUNTRY
    df['Units'] = UNITS
    df['Testing type'] = TESTING_TYPE
    df['Source URL'] = SOURCE_URL
    df['Source label'] = SOURCE_LABEL
    df['Notes'] = ""
    sanity_checks(df)
    df = df[['Country', 'Units', 'Testing type', 'Date', 'Cumulative total', 'Source URL', 'Source label', 'Notes']]
    df.to_csv("automated_sheets/Bangladesh.csv", index=False)

def get_data() -> pd.DataFrame:
    try:
        driver = webdriver.Firefox()
        driver.implicitly_wait(IMPLICIT_WAIT)
        driver.get(DASHBOARD_URL)
        time.sleep(15)
        # searches in all requests made by `DASHBOARD_URL` for the request containing the
        # testing data time series.
        found_testing_data = False
        i = 0
        while (not found_testing_data) and i < len(driver.requests):
            request = driver.requests[i]
            if request.response:
                if 'batchedDataV2' in request.url:
                    try:
                        body = json.loads(request.body)
                        for data_request in body['dataRequest']:
                            for query_field in data_request['datasetSpec']['queryFields']:
                                if query_field['dataTransformation']['sourceFieldName'] == '_Total_Tests_for_COVID19__':
                                    found_testing_data = True
                                    break
                    except:
                        pass
            i += 1
        assert found_testing_data, 'failed to find XHR request containing "_Total_Tests_for_COVID19__" time series data.'
        # extracts json response
        regex = re.search(rb'([\)\]\}\',]{1,10})\n(.*)', request.response.body)
        json_data = json.loads(regex.group(2))
        assert 'default' in json_data
        data = json_data['default']['dataResponse'][2]['dataSubset'][0]['dataset']['tableDataset']
        assert len(data['column']) == 2, 'Expected only two data columns -- retrieved data is probably not the "total tests" time series you are expecting'
        dates = data['column'][0]['stringColumn']['values']
        ts_cumtests = data['column'][1]['longColumn']['values']
        df = pd.DataFrame({'Date': dates, 'Cumulative total': ts_cumtests})
        df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
        df['Cumulative total'] = df['Cumulative total'].astype(int)
    except Exception as e:
        df = None
        print(f'Error in retrieving testing data: {e}')
    finally:
        driver.quit()
    return df

def sanity_checks(df: pd.DataFrame) -> None:
    """checks that there are no obvious errors in the scraped data.
    """
    # checks that the cumulative number of tests on date t is always greater than the figure for t-1:
    assert (df['Cumulative total'].iloc[1:] >= df['Cumulative total'].shift(1).iloc[1:]).all(), "On one or more dates, `Cumulative total` is greater on date t-1."
    # cross-checks a sample of scraped figures against the expected result.
    assert len(official_cumulative_totals) > 0
    for dt, d in official_cumulative_totals:
        val = df.loc[df['Date'] == dt, 'Cumulative total'].squeeze().sum()
        assert val == d['cumulative_total'], f"scraped value ({val:,d}) != official value ({d['cumulative_total']:,d}) on {dt}"

if __name__ == '__main__':
    main()
