import datetime
import re
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import vaxutils


def main():

    # Options for Chrome WebDriver
    op = Options()
    op.add_argument("--headless")

    with webdriver.Chrome(options=op) as driver:

        url = "http://www.health.gov.sc/index.php/figures/"
        driver.get(url)
        time.sleep(5)

        for elem in driver.find_elements_by_class_name("cff-text"):
            if "Seychelles COVID-19 Vaccination" in elem.text:
                text = elem.text

    dateregex = r"Vaccination Campaign figures for (.*?). A cumulative total"
    dateStr = str(re.findall(dateregex, text))
    dateStr = dateStr.replace("nd", "").replace("st", "").replace("th", "").replace("rd", "").replace("'", "")\
        .replace("[", "").replace("]", "")

    dt = dateStr.split()
    day = datetime.datetime.strptime(dt[0], "%d").day
    month = datetime.datetime.strptime(dt[1], "%B").month
    year = datetime.datetime.strptime(dt[2], "%Y").year

    date = str(month) + "-" + str(day) + "-" + str(year)
    date = vaxutils.clean_date(date, "%m-%d-%Y")
    fregex = r"total of (.*?) individuals"
    firstdose = str(re.findall(fregex, text)).replace(",", "")

    sregex = r"COVID-19 and (.*?) individuals"
    seconddose = str(re.findall(sregex, text)).replace(",", "")

    people_vaccinated = vaxutils.clean_count(firstdose)
    people_fully_vaccinated = vaxutils.clean_count(seconddose)
    total_vaccinations = people_vaccinated + people_fully_vaccinated

    vaxutils.increment(
        location="Seychelles",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url="http://www.health.gov.sc/index.php/figures/",
        vaccine="Oxford/AstraZeneca, Sinopharm/Beijing"
    )


if __name__ == "__main__":
    main()