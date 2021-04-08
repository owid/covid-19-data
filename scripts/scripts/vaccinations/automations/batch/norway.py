import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def read_csv_multiple_separators(filepath: str, separators: list, usecols: list) -> pd.DataFrame:
    """Read a csv using potential separator candidates.

    Args:
        filepath (str): Path to file.
        separators (list): List of potential separator candidates. The file is read with the different candidate
                        separators. The one that is most likely to be the actual separator is used. Note that the list 
                        is checked in sequentially.
        usecols (list): Columns to load.

    Returns:
        pandas.DataFrame: Loaded csv
    """
    for sep in separators:
        df = pd.read_csv(filepath, sep=sep)
        if df.shape[1] != 1:
            return df[usecols]
    raise Exception("Check regional settings and the delimiter of the downloaded CSV file.")    


def main():

    url = "https://www.fhi.no/sv/vaksine/koronavaksinasjonsprogrammet/koronavaksinasjonsstatistikk/"

    # Options for Chrome WebDriver
    op = Options()
    op.add_argument("--disable-notifications")
    op.add_argument("--headless")
    op.add_experimental_option("prefs",{
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True 
    })

    with webdriver.Chrome(options=op) as driver:
        driver.implicitly_wait(10)
        # Setting Chrome to trust downloads
        driver.command_executor._commands["send_command"] = ("POST", "/session/$sessionId/chromium/send_command")
        params = {"cmd": "Page.setDownloadBehavior", "params": {"behavior": "allow", "downloadPath": "automations"}}
        command_result = driver.execute("send_command", params)

        driver.get(url)
        driver.execute_script("window.scrollTo(0, 1500)")
        driver.find_element_by_class_name("highcharts-exporting-group").click()

        for item in driver.find_elements_by_class_name("highcharts-menu-item"):
            if item.text == "Last ned CSV":
                item.click()
                time.sleep(2)
                break
    
    df = read_csv_multiple_separators(
        "automations/antall-personer-vaksiner.csv", 
        separators=[";", ","],
        usecols=[
            "Category",
            "Kumulativt antall personer vaksinert med 1.dose",
            "Kumulativt antall personer vaksinert med 2.dose"
        ]
    )

    df = df.rename(columns={
        "Kumulativt antall personer vaksinert med 1.dose": "people_vaccinated",
        "Kumulativt antall personer vaksinert med 2.dose": "people_fully_vaccinated"
    })

    df["total_vaccinations"] = df["people_vaccinated"] + df["people_fully_vaccinated"].fillna(0)

    if "Category" in df.columns:
        df = df.rename(columns={"Category": "date"})
    elif "DateTime" in df.columns:
        df = df.rename(columns={"DateTime": "date"})
        
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")

    df.loc[:, "location"] = "Norway"
    df.loc[:, "vaccine"] = "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
    df.loc[:, "source_url"] = url

    assert len(df) > 10

    df.to_csv("automations/output/Norway.csv", index=False)

    os.remove("automations/antall-personer-vaksiner.csv")

if __name__ == "__main__":
    main()
