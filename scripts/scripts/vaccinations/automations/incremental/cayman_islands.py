import datetime
import re
import requests
from bs4 import BeautifulSoup
import pytz
import vaxutils


def main():

    url = "https://www.exploregov.ky/coronavirus-statistics#vaccine-dashboard"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    total_vaccinations = re.search(r"The total number of COVID-19 vaccines administered to date is ([\d,]+)", soup.text)
    total_vaccinations = vaxutils.clean_count(total_vaccinations.group(1))

    people_fully_vaccinated = re.search(r"The total number of people completing the two-dose course is ([\d,]+)", soup.text)
    people_fully_vaccinated = vaxutils.clean_count(people_fully_vaccinated.group(1))

    people_vaccinated = total_vaccinations - people_fully_vaccinated

    date = str(datetime.datetime.now(pytz.timezone("America/Cayman")).date())

    vaxutils.increment(
        location="Cayman Islands",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url=url,
        vaccine="Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
