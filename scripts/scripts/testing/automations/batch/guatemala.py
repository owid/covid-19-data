"""Constructs daily time series of COVID-19 testing data for Guatemala.

Official dashboard: https://tablerocovid.mspas.gob.gt/."""

import os
import time
import pandas as pd
from glob import glob
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

COUNTRY = "Guatemala"
UNITS = "people tested"
SOURCE_LABEL = "Ministry of Health and Social Assistance"
SOURCE_URL = "https://gtmvigilanciacovid.shinyapps.io/3869aac0fb95d6baf2c80f19f2da5f98"

def main():

    # Options for Chrome WebDriver
    op = Options()
    op.add_argument("--disable-notifications")
    op.add_experimental_option("prefs",{
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True 
    })

    with webdriver.Chrome(options=op) as driver:

        # Setting Chrome to trust downloads
        driver.command_executor._commands["send_command"] = ("POST", "/session/$sessionId/chromium/send_command")
        params = {"cmd": "Page.setDownloadBehavior", "params": {"behavior": "allow", "downloadPath": "tmp"}}
        command_result = driver.execute("send_command", params)

        driver.get(SOURCE_URL)
        time.sleep(3)
        driver.find_element_by_class_name("fa-file-download").click()
        time.sleep(1)
        driver.find_element_by_id("tamizadosFER").click()
        time.sleep(2)

    file = glob("tmp/Tamizados*")[0]
    df = pd.read_csv(file)

    df = df.filter(regex=r"^20[\d-]{8}$")
    df = pd.melt(df).groupby("variable", as_index=False).sum()
    df = df[df["value"] != 0]
    df = df.rename(columns={"variable": "Date", "value": "Daily change in cumulative total"})

    df.loc[:, "Country"] = COUNTRY
    df.loc[:, "Units"] = UNITS
    df.loc[:, "Source label"] = SOURCE_LABEL
    df.loc[:, "Source URL"] = SOURCE_URL
    df.loc[:, "Notes"] = pd.NA

    df.to_csv("automated_sheets/Guatemala.csv", index=False)
    os.remove(file)

if __name__ == "__main__":
    main()
