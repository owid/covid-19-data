"""retrieves time series of daily COVID-19 tests conducted in Switzerland.
Data source: https://covid-19-schweiz.bagapps.ch/de-3.html
"""

import requests
import traceback
import time
import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

def main():
    URL = "https://public.tableau.com/views/COVID-19_Dash3/Dashboard3d?:embed=y&:showVizHome=no&:host_url=https%3A%2F%2Fpublic.tableau.com%2F&:embed_code_version=3&:tabs=no&:toolbar=yes&:animate_transition=yes&:display_static_image=no&:display_spinner=no&:display_overlay=yes&:display_count=yes&publish=yes&:loadOrderID=0"
    COUNTRY = "Switzerland"
    UNITS = "tests performed"
    SOURCE_URL = "https://covid-19-schweiz.bagapps.ch/de-3.html"
    SOURCE_LABEL = "Federal Office of Public Health"

    df = _get_data(URL)
    df.columns = df.columns.str.lower().str.strip()
    df['datum'] = pd.to_datetime(df['datum'], format="%d/%m/%Y")
    df = df[-df["anzahl_tests"].isnull()]
    _do_initial_sanity_checks(df)
    df_final = df.groupby('datum')['anzahl_tests'].sum().reset_index()
    df_final.rename(columns={'datum': 'Date', 'anzahl_tests': 'Daily change in cumulative total'}, inplace=True)
    df_final['Country'] = COUNTRY
    df_final['Units'] = UNITS
    df_final['Source URL'] = SOURCE_URL
    df_final['Source label'] = SOURCE_LABEL
    df_final['Notes'] = np.nan
    df_final['Testing type'] = 'PCR only'
    _do_final_sanity_checks(df_final)
    df_final.sort_values('Date', inplace=True)
    df_final.to_csv('automated_sheets/Switzerland.csv', index=False)
    return None

def _get_data(URL):
    try:
        driver = webdriver.Firefox()
        driver.get(URL)
        time.sleep(5)  # KLUDGE: to give time for page to fully load before proceeding..
        graph = WebDriverWait(driver, 20).until(
                              EC.element_to_be_clickable((By.ID, "tabZoneId56")))
        graph.find_element_by_class_name("tab-tvLeftAxis").click()
        time.sleep(3)  # KLUDGE: tableau seems to need a couple seconds here to register that the csv data is now downloadable.
        download_button = WebDriverWait(driver, 20).until(
                                        EC.element_to_be_clickable((By.XPATH, '//span[@class="tabToolbarButtonImg tab-icon-download"]')))
        download_button.click()
        data_button = WebDriverWait(driver, 20).until(
                                    EC.element_to_be_clickable((By.XPATH, '//button[@data-tb-test-id="DownloadData-Button"]')))
        data_button.click()
        # window_before = driver.window_handles[0]
        window_after = driver.window_handles[1]
        driver.switch_to_window(window_after)
        a = WebDriverWait(driver, 15).until(lambda browser: driver.find_element_by_class_name("csvLink_summary"))
        # tab = ui.WebDriverWait(driver, 15).until(lambda browser: driver.find_element_by_id('tab-view-full-data'))
        # tab.click()
        # a = ui.WebDriverWait(driver, 15).until(lambda browser: driver.find_element_by_class_name("csvLink"))
        csv_url = a.get_attribute('href')
        df = pd.read_csv(csv_url, sep=";")
    except Exception:
        traceback.print_exc()
        raise RuntimeError('Failed to retrieve data.')
    finally:
        driver.quit()
    return df

def _do_initial_sanity_checks(df: pd.DataFrame):
    assert not df.columns.duplicated().any(), "One or more duplicate columns exist in df."
    assert is_numeric_dtype(df["anzahl_tests"]), "anzahl_tests is not of type int64"
    assert df.shape[0] > 30, f"There should be at least 30 days (rows) of data, but only {df.shape[0]} rows are in df."
    # each day should have two rows of data - one for negative tests and one for positive
    assert (df.groupby('datum')['anzahl_tests'].count() == 2).all(), "One or more days has > 2 rows of data."

def _do_final_sanity_checks(df: pd.DataFrame):
    required_columns = ['Date', 'Country',  'Units', 'Source URL', 'Source label', 'Notes', 'Daily change in cumulative total']
    assert all([col in df.columns for col in required_columns]), "One or more requred columns is not in df_final"

if __name__ == '__main__':
    main()
