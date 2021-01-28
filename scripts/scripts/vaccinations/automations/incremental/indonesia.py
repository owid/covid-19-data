import re
import requests
from bs4 import BeautifulSoup
import dateparser
import vaxutils


def main():

    url = "https://www.kemkes.go.id/"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    date = soup.find(class_="covid-case-container").find(class_="info-date").text.replace("Kondisi ", "")
    date = str(dateparser.parse(date, languages=["id"]).date())

    people_vaccinated = soup.find(class_="description", text="Vaksinasi-1").parent.find(class_="case").text
    people_vaccinated = vaxutils.clean_count(people_vaccinated)

    people_fully_vaccinated = soup.find(class_="description", text="Vaksinasi-2").parent.find(class_="case").text
    people_fully_vaccinated = vaxutils.clean_count(people_fully_vaccinated)

    total_vaccinations = people_vaccinated + people_fully_vaccinated

    vaxutils.increment(
        location="Indonesia",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url=url,
        vaccine="Sinovac"
    )


if __name__ == '__main__':
    main()
