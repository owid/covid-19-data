import re
import requests
from bs4 import BeautifulSoup
import vaxutils


def main():

    url = "https://covid19.gov.im/general-information/latest-updates/"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    vax_box = soup.find(string=re.compile("Total first vaccinations")).parent.parent

    date = vax_box.find("strong").text
    date = vaxutils.clean_date(date, "%d %B %Y")

    for p in vax_box.find_all("p"):
        if "Total first vaccinations" in p.text:
            data = p.text

    people_vaccinated, people_fully_vaccinated = re.search(r"Total first vaccinations:\xa0 ([\d,]+)Total second vaccinations:\xa0 ([\d,]+)", data).groups()

    people_vaccinated = vaxutils.clean_count(people_vaccinated)
    people_fully_vaccinated = vaxutils.clean_count(people_fully_vaccinated)
    total_vaccinations = people_vaccinated + people_fully_vaccinated

    vaxutils.increment(
        location="Isle of Man",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url=url,
        vaccine="Pfizer/BioNTech"
    )


if __name__ == '__main__':
    main()
