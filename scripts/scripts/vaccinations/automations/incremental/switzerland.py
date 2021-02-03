import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
import vaxutils


def main():

    url = "https://www.covid19.admin.ch/en/epidemiologic/vacc-doses"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    table = soup.find(class_="geo-unit-vacc-doses-data__table")
    df = pd.read_html(str(table))[0]

    total_vaccinations = df.loc[df["per 100 inhabitants"] == "Administered vaccines", "absolute numbers"].values[0]
    total_vaccinations = vaxutils.clean_count(total_vaccinations)

    date = soup.find(class_="detail-card__source").find("span").text
    date = re.search(r"[\d\.]{10}", date).group(0)
    date = vaxutils.clean_date(date, "%d.%m.%Y")

    vaxutils.increment(
        location="Switzerland",
        total_vaccinations=total_vaccinations,
        date=date,
        source_url=url,
        vaccine="Moderna, Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
