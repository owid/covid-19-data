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

    table = soup.find("p", string=re.compile("Поставени ваксини по")).parent.find("table")
    df = pd.read_html(str(table))[0]
    df = df.droplevel(level=0, axis=1)
    df = df[df["Област"] == "Общо"]

    total_vaccinations = int(df["Общо поставени дози"].values[0])
    people_fully_vaccinated = int(df["Общо ваксинирани лицас втора доза"].values[0])
    people_vaccinated = total_vaccinations - people_fully_vaccinated

    date = str(datetime.datetime.now(pytz.timezone("Europe/Sofia")).date() - datetime.timedelta(days=1))

    vaxutils.increment(
        location="Bulgaria",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url=url,
        vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
