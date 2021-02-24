import datetime
import requests

from bs4 import BeautifulSoup
import pytz

import vaxutils


def main():

    url = "https://saudimoh.maps.arcgis.com/sharing/rest/content/items/26691a58d8e74c1aa6d5de967caa937f/data?f=json"
    widget_id = "2c37ab9e-96ed-4092-8e22-c33da3042d8c"

    data = requests.get(url).json()

    for widget in data["widgets"]:
        if widget["id"] == widget_id:
            break

    soup = BeautifulSoup(widget["text"], "html.parser")
    total_vaccinations = soup.find("strong").text
    total_vaccinations = vaxutils.clean_count(total_vaccinations)

    date = str(datetime.datetime.now(pytz.timezone("Asia/Riyadh")).date())

    vaxutils.increment(
        location="Saudi Arabia",
        total_vaccinations=total_vaccinations,
        date=date,
        source_url="https://covid19.moh.gov.sa/",
        vaccine="Pfizer/BioNTech",
    )


if __name__ == '__main__':
    main()
