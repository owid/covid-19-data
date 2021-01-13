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

    date = soup.find(class_="field-name-post-date").text
    date = vaxutils.clean_date(date, "%d.%m.%Y")

    count = soup.find(string=re.compile(r"Eestis on COVID-19 vastu vaktsineerimisi"))
    count = re.search(r"tehtud ([\d\s]+) inimesele", count).group(1)
    count = vaxutils.clean_count(count)

    vaxutils.increment(
        location="Estonia",
        total_vaccinations=count,
        date=date,
        source_url=url,
        vaccine="Pfizer/BioNTech"
    )

if __name__ == "__main__":
    main()
