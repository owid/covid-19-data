import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd

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
        params = {"cmd": "Page.setDownloadBehavior", "params": {"behavior": "allow", "downloadPath": "input"}}
        command_result = driver.execute("send_command", params)

        driver.get("https://e.infogram.com/79acdebc-18ad-4399-8936-bce3b3a49068")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 750)")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, 1500)")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, 2250)")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, 3000)")
        time.sleep(1)
        driver.find_elements_by_class_name("igc-data-download")[3].click()
        time.sleep(2)

    df = pd.read_csv("input/data.csv")
    df.columns = ["Date", "Daily change in cumulative total"]
    df.loc[:, "Date"] = pd.to_datetime(
        (df.Date + "-2020")
        .str.replace("Ene", "Jan")
        .str.replace("Abr", "Apr")
        .str.replace("Ago", "Aug")
    )

    df.loc[:, "Daily change in cumulative total"] = df["Daily change in cumulative total"].astype(int)
    df.loc[:, "Country"] = "Chile"
    df.loc[:, "Source URL"] = "https://www.gob.cl/coronavirus/cifrasoficiales/#reportes"
    df.loc[:, "Source label"] = "Ministry of Health"
    df.loc[:, "Units"] = "tests performed"
    df.loc[:, "Notes"] = pd.NA
    df.loc[:, "Testing type"] = "PCR only"

    df.to_csv("automated_sheets/Chile.csv", index=False)

if __name__ == "__main__":
    main()
