import re
import requests
from bs4 import BeautifulSoup
import vaxutils


def main():

    url = "https://www.terviseamet.ee/et/uudised"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    for h2 in soup.find_all("h2", class_="views-field-title"):
        if "COVID-19 blogi" in h2.text:
            url = "https://www.terviseamet.ee" + h2.find("a")["href"]
            break

    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    text = soup.find(class_="node-published").text

    people_vaccinated, people_fully_vaccinated = re.search(
        r"Eestis on COVID-19 vastu vaktsineerimisi tehtud ([\d\s]+) inimesele, kaks doosi on saanud ([\d\s]+) inimest",
        text
    ).groups()

    people_vaccinated = vaxutils.clean_count(people_vaccinated)
    people_fully_vaccinated = vaxutils.clean_count(people_fully_vaccinated)
    total_vaccinations = people_vaccinated + people_fully_vaccinated

    date = soup.find(class_="field-name-post-date").text
    date = vaxutils.clean_date(date, "%d.%m.%Y")

    vaxutils.increment(
        location="Estonia",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url=url,
        vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
