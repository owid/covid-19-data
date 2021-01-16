import datetime
import pytz
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
import vaxutils

def main():

    url = "https://coronavirus.bg/bg/statistika"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    table = soup.find("p", string=re.compile("Ваксинирани лица по")).parent.find("table")
    df = pd.read_html(str(table))[0]
    df = df.droplevel(level=0, axis=1)

    count = df.loc[df["Област"] == "Общо", "Общо"].values[0]
    count = int(count)

    date = str(datetime.datetime.now(pytz.timezone("Europe/Sofia")).date() - datetime.timedelta(days=1))

    vaxutils.increment(
        location="Bulgaria",
        total_vaccinations=count,
        date=date,
        source_url=url,
        vaccine="Moderna, Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
