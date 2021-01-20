import pytz
import datetime
import requests
from bs4 import BeautifulSoup
import vaxutils

def main():
    url = "http://covid19.ncema.gov.ae/en"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    total_vaccinations = soup.find(class_="doses").find(class_="counter").text
    total_vaccinations = vaxutils.clean_count(total_vaccinations)

    # https://www.khaleejtimes.com/coronavirus-pandemic/covid-vaccine-uae-250000-residents-get-second-dose
    people_fully_vaccinated = 250000

    people_vaccinated = total_vaccinations - people_fully_vaccinated

    date = str(datetime.datetime.now(pytz.timezone("Asia/Dubai")).date())

    vaxutils.increment(
        location="United Arab Emirates",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url=url,
        vaccine="Pfizer/BioNTech, Sinopharm"
    )


if __name__ == "__main__":
    main()
