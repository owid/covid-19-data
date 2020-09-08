import datetime
from selenium import webdriver
import pandas as pd

def main():
    driver = webdriver.Firefox()
    driver.get("https://covid19.saglik.gov.tr/")
    count = driver.find_element_by_class_name("toplam-test-sayisi").text.replace(".", "")
    count = int(count)

    existing = pd.read_csv("automated_sheets/Turkey.csv").sort_values("Date", ascending=False)
    
    if count > new["Cumulative total"][0]:
        new = existing.head(1).copy()
        new.loc[:, "Date"] = datetime.date.today()
        new.loc[:, "Cumulative total"] = count
        new_df = pd.concat([new, existing], ignore_index=True)
        new_df.to_csv("automated_sheets/Turkey.csv", index=False)

    driver.close()

if __name__ == "__main__":
    main()
