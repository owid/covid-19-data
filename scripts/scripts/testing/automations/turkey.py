import datetime
from selenium import webdriver
import pandas as pd

def main():
    str_today = str(datetime.date.today())
    existing = pd.read_csv("automated_sheets/Turkey.csv").sort_values("Date", ascending=False)

    if str_today > existing["Date"][0]:

        driver = webdriver.Firefox()
        driver.get("https://covid19.saglik.gov.tr/")
        count = driver.find_element_by_class_name("toplam-test-sayisi").text.replace(".", "")
        count = int(count)
        
        if count > existing["Cumulative total"][0]:
            new = existing.head(1).copy()
            new.loc[:, "Date"] = str_today
            new.loc[:, "Cumulative total"] = count
            new_df = pd.concat([new, existing], ignore_index=True)
            new_df.to_csv("automated_sheets/Turkey.csv", index=False)

        driver.close()

if __name__ == "__main__":
    main()
