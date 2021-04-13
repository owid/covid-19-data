import re
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from vax.utils.incremental import enrich_data, increment, clean_date, clean_count


def main():

    data = {
        "location": "Guatemala",
        "source_url": "https://gtmvigilanciacovid.shinyapps.io/3869aac0fb95d6baf2c80f19f2da5f98",
        "vaccine": "Moderna, Oxford/AstraZeneca",
    }

    op = Options()
    op.add_argument("--headless")
    with webdriver.Chrome(options=op) as driver:
        driver.maximize_window()  # For maximizing window
        driver.implicitly_wait(20)  # gives an implicit wait for 20 seconds
        driver.get(data["source_url"])
        driver.find_element_by_class_name("fa-syringe").click()
        date = driver.find_element_by_class_name("logo").text
        dose1 = driver.find_element_by_id("dosisaplicadas1").find_element_by_tag_name("h3").text
        dose2 = driver.find_element_by_id("dosisaplicadas2").find_element_by_tag_name("h3").text

    data["people_vaccinated"] = clean_count(dose1)
    data["people_fully_vaccinated"] = clean_count(dose2)
    data["total_vaccinations"] = data["people_vaccinated"] + data["people_fully_vaccinated"]

    date = re.search(r"\d+/\d+/202\d", date).group(0)
    data["date"] = clean_date(date, "%d/%m/%Y")

    increment(
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
