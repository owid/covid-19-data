import re
import requests
from bs4 import BeautifulSoup
import dateparser
import pandas as pd
import vaxutils


def main():

    url = "https://www.folkhalsomyndigheten.se/smittskydd-beredskap/utbrott/aktuella-utbrott/covid-19/vaccination-mot-covid-19/statistik-over-vaccinerade-mot-covid-19/"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    table = soup.find(id="content-primary").find("table")
    df = pd.read_html(str(table))[0]
    assert len(df) == 1

    total_vaccinations = df["Alla vacciner"].values[0]
    total_vaccinations = vaxutils.clean_count(total_vaccinations)

    date = soup.find(id="content-primary").find("h2").text
    date = re.search(r"\d+\s\w+\s+202\d", date).group(0)
    date = str(dateparser.parse(date, languages=["sv"]).date())

    vaxutils.increment(
        location="Sweden",
        total_vaccinations=total_vaccinations,
        date=date,
        source_url=url,
        vaccine="Pfizer/BioNTech"
    )


if __name__ == '__main__':
    main()
