import datetime
import re
import requests
from bs4 import BeautifulSoup
import vaxutils

def main():

    url = "https://thl.fi/en/web/infectious-diseases-and-vaccinations/what-s-new/coronavirus-covid-19-latest-updates/situation-update-on-coronavirus"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    for tr in soup.find(class_="journal-content-article").find("table").find_all("tr"):
        if "Nationwide total" in tr.text:
            break

    count = tr.find_all("td")[1].text
    count = vaxutils.clean_count(count)

    date = str(datetime.date.today())

    vaxutils.increment(
        location="Finland",
        total_vaccinations=count,
        date=date,
        source_url=url,
        vaccine="Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
