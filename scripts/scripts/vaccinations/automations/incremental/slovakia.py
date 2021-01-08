import re
import requests
from bs4 import BeautifulSoup
import vaxutils

def main():

    url = "https://korona.gov.sk/koronavirus-na-slovensku-v-cislach/"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    count = soup.find(string=re.compile("Počet zaočkovaných osôb")).parent.parent.parent.find("h3").text
    count = vaxutils.clean_count(count)

    date = soup.find(string=re.compile("Počet zaočkovaných osôb")).parent.parent.text
    date = re.search(r"\d+\.\d+\.\d+", date).group(0)
    date = vaxutils.clean_date(date, "%d.%m.%Y")

    vaxutils.increment(
        location="Slovakia",
        total_vaccinations=count,
        date=date,
        source_url=url,
        vaccine="Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
