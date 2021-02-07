import datetime
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import vaxutils


def main():

    # Options for Chrome WebDriver
    op = Options()
    op.add_argument("--headless")

    with webdriver.Chrome(options=op) as driver:

        url = "https://covid.hespress.com/fr/"
        driver.get(url)
        time.sleep(5)

        for elem in driver.find_elements_by_class_name("card-title"):
            if "Bénéficiaires de la vaccination" in elem.text:
                cnt = elem.find_element_by_xpath("//h5/following-sibling::h4")
                people_vaccinated = vaxutils.clean_count(cnt.text)
                total_vaccinations = vaxutils.clean_count(cnt.text)

    people_vaccinated = people_vaccinated
    total_vaccinations = total_vaccinations

    date = datetime.date.today()
    date = str(date)

    vaxutils.increment(
        location="Western Sahara",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        date=date,
        source_url=url,
        vaccine="Oxford/AstraZeneca, Sinopharm"
    )


if __name__ == "__main__":
    main()