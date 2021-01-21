import re
import requests
from bs4 import BeautifulSoup
import vaxutils


def main():

    url = "https://covid19.gov.im/general-information/latest-updates/"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    date = soup.find(class_="fa-syringe").parent.find("strong").text
    date = vaxutils.clean_date(date, "%d %B %Y")

    total_vaccinations = soup.find(class_="fa-syringe").parent.text
    total_vaccinations = re.search(r"Total vaccinations:\s+(\d+)", total_vaccinations).group(1)
    total_vaccinations = vaxutils.clean_count(total_vaccinations)

    vaxutils.increment(
        location="Isle of Man",
        total_vaccinations=total_vaccinations,
        date=date,
        source_url=url,
        vaccine="Pfizer/BioNTech"
    )


if __name__ == '__main__':
    main()
