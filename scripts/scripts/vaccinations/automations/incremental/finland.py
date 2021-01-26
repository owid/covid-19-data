import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
import vaxutils


def main():

    url = "https://www.thl.fi/episeuranta/rokotukset/koronarokotusten_edistyminen.html"
    soup = BeautifulSoup(requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}).content, "html.parser")

    table = soup.find("table")
    df = pd.read_html(str(table))[0]
    df = df[df["Sairaanhoitopiiri"] == "Kaikki"]

    people_vaccinated = int(df["Ensimmäisen annoksen saaneet"].values[0])
    people_fully_vaccinated = int(df["Toisen annoksen saaneet"].values[0])
    total_vaccinations = int(df["Annetut annokset yhteensä"].values[0])

    date = soup.find(class_="date").text
    date = re.search(r"[\d-]{10}", date).group(0)

    vaxutils.increment(
        location="Finland",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url=url,
        vaccine="Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
