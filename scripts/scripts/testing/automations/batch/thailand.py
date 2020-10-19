import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


SOURCE_URL = "https://www3.dmsc.moph.go.th/"


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

        driver.get(SOURCE_URL)
        nextcloud = (
            driver.find_elements_by_css_selector(".app-body .bg-white .container center a")[-1]
            .get_attribute("href")
        )
        driver.get(nextcloud)
        driver.find_element_by_css_selector(".directDownload a").click()
        time.sleep(2)

    df = pd.read_excel("input/Thailand_COVID-19_testing_data.xlsx")
    df.loc[:, "Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df[["Date", "Total"]].dropna()
    df = df[df["Total"] > 0]
    df = df.rename(columns={"Total": "Daily change in cumulative total"})

    df.loc[:, "Daily change in cumulative total"] = df["Daily change in cumulative total"].astype(int)
    df.loc[:, "Country"] = "Thailand"
    df.loc[:, "Source URL"] = SOURCE_URL
    df.loc[:, "Source label"] = "Ministry of Public Health"
    df.loc[:, "Units"] = "tests performed"
    df.loc[:, "Notes"] = pd.NA
    df.loc[:, "Testing type"] = "PCR only"

    df.to_csv("automated_sheets/Thailand.csv", index=False)


if __name__ == '__main__':
    main()
