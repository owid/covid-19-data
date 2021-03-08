import re
import time

import pandas as pd
from selenium import webdriver

import vaxutils


def main():

    data = {
        "location": "Guatemala",
        "source_url": "https://gtmvigilanciacovid.shinyapps.io/3869aac0fb95d6baf2c80f19f2da5f98",
        "vaccine": "Moderna",
    }

    with webdriver.Chrome() as driver:
        driver.get(data["source_url"])
        time.sleep(2)
        driver.find_element_by_class_name("fa-syringe").click()
        time.sleep(2)
        date = driver.find_element_by_class_name("logo").text
        tbl = driver.find_element_by_class_name("dataTables_scrollBody").get_attribute("innerHTML")

    df = pd.read_html(tbl)[0]

    data["people_vaccinated"] = df["Primera dosis"].tail(1).item()
    data["people_fully_vaccinated"] = df["Segunda dosis"].tail(1).item()
    data["total_vaccinations"] = data["people_vaccinated"] + data["people_fully_vaccinated"]

    date = re.search(r"\d+/\d+/202\d", date).group(0)
    data["date"] = vaxutils.clean_date(date, "%d/%m/%Y")

    vaxutils.increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"],
    )


if __name__ == "__main__":
    main()
