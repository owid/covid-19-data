import os
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import vaxutils

def main():
     
    page = requests.get("https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Impfquoten-Tab.html")
    soup = BeautifulSoup(page.content, "html.parser")

    url = soup.find(id="main").find(class_="box").find("ul", class_="links").find("a")["href"]
    url = "https://www.rki.de" + url

    with open("germany.xlsx", "wb") as file:
        file.write(requests.get(url).content)

    df = pd.ExcelFile("germany.xlsx")
    sheet_names = df.sheet_names
    sheet_names.remove("Erläuterung")
    assert len(sheet_names) == 1

    date = sheet_names[0]

    df = df.parse(date)
    count = int(df.loc[df["Bundesland"] == "Gesamt", "Impfungen kumulativ"].values[0])

    date = str(pd.to_datetime(date, format="%d.%m.%y").date())

    vaxutils.increment(
        location="Germany",
        total_vaccinations=count,
        date=date,
        source_url="https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Impfquoten-Tab.html",
        vaccine="Pfizer/BioNTech"
    )

    os.remove("germany.xlsx")


if __name__ == "__main__":
    main()
