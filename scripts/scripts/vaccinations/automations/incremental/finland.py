import datetime
import re
import requests
from bs4 import BeautifulSoup
import vaxutils

def main():

    url = "https://thl.fi/fi/web/infektiotaudit-ja-rokotukset/ajankohtaista/ajankohtaista-koronaviruksesta-covid-19/tilannekatsaus-koronaviruksesta"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    for p in soup.find(class_="journal-content-article").find_all("p"):
        if "Annetut rokoteannokset" in p.text:
            break

    count = p.find("strong").text
    count = vaxutils.clean_count(count)

    date = soup.find(class_="thl-image-caption").text
    date = re.compile(r"Tiedot on päivitetty (\d+)\.(\d+)").search(date)
    date = datetime.date(year=2021, month=int(date.group(2)), day=int(date.group(1)))
    date = str(date)

    vaxutils.increment(
        location="Finland",
        total_vaccinations=count,
        date=date,
        source_url=url,
        vaccine="Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
