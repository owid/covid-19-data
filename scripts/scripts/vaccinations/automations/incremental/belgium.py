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
        doseArr = []
        url = "https://datastudio.google.com/embed/reporting/c14a5cfc-cab7-4812-848c-0369173148ab/page/hOMwB"
        driver.get(url)
        time.sleep(10)
        page_source = driver.page_source
        for elem in driver.find_elements_by_class_name("kpimetric"):
            if "%" not in str(elem.find_element_by_class_name("valueLabel").text):
                doseArr.append(elem.find_element_by_class_name("valueLabel").text)
    total_vaccinations = vaxutils.clean_count(doseArr[0]) + vaxutils.clean_count(doseArr[1])
    people_vaccinated = vaxutils.clean_count(doseArr[0])

    regex = r"numbers of  (.*?),"
    dateStr = re.findall(regex, page_source)
    dateStr = dateStr[0]
    date_month = datetime.datetime.strptime(str(dateStr.split()[1]), "%B")
    month_number = date_month.month

    date = str(month_number) + "-" + str(dateStr.split()[0]) + "-" + str(datetime.date.today().year)
    date = vaxutils.clean_date(date, "%m-%d-%Y")

    vaxutils.increment(
        location="Belgium",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        date=date,
        source_url=url,
        vaccine="Moderna, Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()