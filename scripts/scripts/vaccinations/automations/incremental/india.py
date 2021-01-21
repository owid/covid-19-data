import datetime
import re
import requests
from bs4 import BeautifulSoup
import vaxutils


def main():

    url = "https://www.mohfw.gov.in/"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    total_vaccinations = soup.find(class_="coviddata").text
    total_vaccinations = vaxutils.clean_count(total_vaccinations)

    date = soup.find(id="site-dashboard").find("h5").text
    date = re.search(r"\d+\s\w+\s+202\d", date).group(0)
    date = datetime.datetime.strptime(date, "%d %B %Y") - datetime.timedelta(days=1)
    date = str(date.date())

    vaxutils.increment(
        location="India",
        total_vaccinations=total_vaccinations,
        date=date,
        source_url=url,
        vaccine="Covaxin, Covishield"
    )


if __name__ == '__main__':
    main()
