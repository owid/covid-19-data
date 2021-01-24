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

    total_vaccinations = soup.find(class_="description", text="Divaksin").parent.find(class_="case").text
    total_vaccinations = vaxutils.clean_count(total_vaccinations)

    vaxutils.increment(
        location="Indonesia",
        total_vaccinations=total_vaccinations,
        date=date,
        source_url=url,
        vaccine="Sinovac"
    )


if __name__ == '__main__':
    main()
