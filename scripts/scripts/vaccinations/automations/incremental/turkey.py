import datetime
import pytz
import re
import requests
from bs4 import BeautifulSoup
import vaxutils


def main():

    url = "https://covid19asi.saglik.gov.tr/"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    count = re.search(r"var asiyapilankisisayisi = (\d+);", str(soup)).group(1)
    count = vaxutils.clean_count(count)

    date = str(datetime.datetime.now(pytz.timezone("Asia/Istanbul")).date())

    vaxutils.increment(
        location="Turkey",
        total_vaccinations=count,
        date=date,
        source_url=url,
        vaccine="Sinovac"
    )


if __name__ == '__main__':
    main()
