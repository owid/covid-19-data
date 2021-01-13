import pytz
import datetime
import requests
from bs4 import BeautifulSoup
import vaxutils

def main():
    url = "http://covid19.ncema.gov.ae/en"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    count = soup.find(class_="doses").find(class_="counter").text
    count = vaxutils.clean_count(count)
    date = str(datetime.datetime.now(pytz.timezone("Asia/Dubai")).date())

    vaxutils.increment(
        location="United Arab Emirates",
        total_vaccinations=count,
        date=date,
        source_url=url,
        vaccine="Sinopharm"
    )


if __name__ == "__main__":
    main()
