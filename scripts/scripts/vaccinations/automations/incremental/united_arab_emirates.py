import pytz
import datetime
import requests
from bs4 import BeautifulSoup
import vaxutils

def main():
    url = "http://covid19.ncema.gov.ae/en"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    total_vaccinations = soup.find(class_="doses").find(class_="counter").text
    total_vaccinations = vaxutils.clean_count(total_vaccinations)

    date = str(datetime.datetime.now(pytz.timezone("Asia/Dubai")).date() - datetime.timedelta(days=1))

    vaxutils.increment(
        location="United Arab Emirates",
        total_vaccinations=total_vaccinations,
        # people_vaccinated=people_vaccinated,
        # people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url=url,
        vaccine="Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sinopharm/Wuhan, Sputnik V"
    )


if __name__ == "__main__":
    main()
