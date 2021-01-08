import datetime
import pytz
import requests
from bs4 import BeautifulSoup
import vaxutils

def main():

    url = "https://coronavirus.bg/bg/statistika"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    count = (
        soup
        .find(class_="statistics-label", text="Ваксинирани")
        .parent
        .find(class_="statistics-value")
        .text
    )
    count = vaxutils.clean_count(count)

    date = str(datetime.datetime.now(pytz.timezone("Europe/Sofia")).date())

    vaxutils.increment(
        location="Bulgaria",
        total_vaccinations=count,
        date=date,
        source_url=url,
        vaccine="Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
