import re
import requests

from bs4 import BeautifulSoup

import vaxutils


def main():

    url = "https://vaccinocovid.iss.sm/"
    soup = BeautifulSoup(requests.get(url, verify=False).content, "html.parser")

    for script in soup.find_all("script"):
        if "new Chart" in str(script):
            chart_data = str(script)
            break

    people_vaccinated = re.search(r"([\d,. ]+) [Vv]accinati", chart_data).group(1)
    people_vaccinated = vaxutils.clean_count(people_vaccinated)

    people_fully_vaccinated = 0

    total_vaccinations = people_vaccinated + people_fully_vaccinated

    date = re.search(r"Dati aggiornati al (\d{2}/\d{2}/\d{4})", chart_data).group(1)
    date = vaxutils.clean_date(date, "%d/%m/%Y")

    vaxutils.increment(
        location="San Marino",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url="https://vaccinocovid.iss.sm/",
        vaccine="Sputnik V"
    )


if __name__ == '__main__':
    main()
