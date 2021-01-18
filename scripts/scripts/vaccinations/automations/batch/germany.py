import os
import requests
from bs4 import BeautifulSoup
import pandas as pd

def main():
     
    url = "https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Impfquoten-Tab.html"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    url = soup.find(id="main").find(class_="sectionRelated").find("a")["href"]
    url = "https://www.rki.de" + url

    with open("germany.xlsx", "wb") as file:
        file.write(requests.get(url).content)

    df = pd.read_excel("germany.xlsx", sheet_name="Impfungen_proTag")
    df = df.rename(columns={
        "Datum": "date",
        "Erstimpfung": "people_vaccinated",
        "Zweitimpfung": "people_fully_vaccinated"
    })
    df = df[(-df["date"].isna()) & (df["date"] != "Impfungen gesamt")].copy()
    df["date"] = df["date"].astype(str).str.slice(0, 10)
    df = df.sort_values("date")

    df["people_vaccinated"] = df["people_vaccinated"].cumsum()
    df["people_fully_vaccinated"] = df["people_fully_vaccinated"].cumsum()
    df["total_vaccinations"] = df["people_vaccinated"] + df["people_fully_vaccinated"]

    df.loc[:, "location"] = "Germany"
    df.loc[:, "source_url"] = url

    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[df["date"] >= "2021-01-12", "vaccine"] = "Moderna, Pfizer/BioNTech"

    df.to_csv("automations/output/Germany.csv", index=False)

    os.remove("germany.xlsx")

if __name__ == "__main__":
    main()
