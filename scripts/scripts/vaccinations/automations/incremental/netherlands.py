import re
import requests
from bs4 import BeautifulSoup
import dateparser
import pandas as pd
import vaxutils


def main():

    url = "https://www.rivm.nl/covid-19-vaccinatie/cijfers-vaccinatieprogramma"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    table = soup.find(id="main-content").parent.find("table")
    df = pd.read_html(str(table), thousands=".")[0]

    total_vaccinations = df.loc[df["Doelgroep"] == "Totaal", "Aantal personen bij wie de vaccinatie gestart is"].values[0]
    total_vaccinations = int(total_vaccinations)

    date = soup.find(string=re.compile("Vaccinatiecijfers.*202\\d"))
    date = re.search(r"\d+\s\w+\s+202\d", date).group(0)
    date = str(dateparser.parse(date, languages=["nl"]).date())

    vaxutils.increment(
        location="Netherlands",
        total_vaccinations=total_vaccinations,
        date=date,
        source_url=url,
        vaccine="Pfizer/BioNTech"
    )


if __name__ == '__main__':
    main()
