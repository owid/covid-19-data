import datetime
import pytz
import re
import requests
from bs4 import BeautifulSoup
import vaxutils


def main():

    url = "https://covid19asi.saglik.gov.tr/"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    total_vaccinations = re.search(r"var yapilanasisayisi = (\d+);", str(soup)).group(1)
    total_vaccinations = vaxutils.clean_count(total_vaccinations)

    people_vaccinated = re.search(r"var asiyapilankisisayisi1Doz = (\d+);", str(soup)).group(1)
    people_vaccinated = vaxutils.clean_count(people_vaccinated)

    people_fully_vaccinated = re.search(r"var asiyapilankisisayisi2Doz = (\d+);", str(soup)).group(1)
    people_fully_vaccinated = vaxutils.clean_count(people_fully_vaccinated)

    date = str(datetime.datetime.now(pytz.timezone("Asia/Istanbul")).date())

    vaxutils.increment(
        location="Turkey",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url=url,
        vaccine="Sinovac"
    )


if __name__ == '__main__':
    main()
