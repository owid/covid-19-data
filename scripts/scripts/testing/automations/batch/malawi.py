import datetime
import re
import time
import pandas as pd
from selenium import webdriver

SOURCE_URL = "https://covid19.health.gov.mw/"

def main():

    with webdriver.Firefox() as driver:
        driver.get(SOURCE_URL)
        time.sleep(5)

        boxes = driver.find_elements_by_class_name("e-parent")
        for box in boxes:
            inner_text = box.get_attribute("innerText")
            if "Total Samples Tested" in inner_text:
                tests_cumul = int(re.search(r"\d+", inner_text.replace(u"\u202f", "")).group(0))
                break

    existing = pd.read_csv("automated_sheets/Malawi.csv")
    date = str(datetime.date.today())

    if date > existing["Date"].max() and tests_cumul > existing["Cumulative total"].max():

        new = pd.DataFrame({
            "Country": "Malawi",
            "Date": [date],
            "Cumulative total": tests_cumul,
            "Source URL": SOURCE_URL,
            "Source label": "Ministry Of Health - Malawi",
            "Units": "samples tested",
            "Notes": pd.NA
        })

        df = pd.concat([new, existing]).sort_values("Date", ascending=False)
        df.to_csv("automated_sheets/Malawi.csv", index=False)

if __name__ == "__main__":
    main()
