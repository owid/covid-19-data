import os
import requests
from bs4 import BeautifulSoup
import pandas as pd


def main():
     
    url = "https://onemocneni-aktualne.mzcr.cz/vakcinace-cr"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")
    data_link = soup.find("a", text="Data ke stažení").attrs["href"]

    df = pd.read_csv(data_link, parse_dates=["datum_vakcinace"])
    assert df.columns.tolist() == ["datum_vakcinace", "vykázaná očkování"]
    df = df.rename(columns={
        "datum_vakcinace": "date",
        "vykázaná očkování": "total_vaccinations",
    })
    df["date"] = df["date"].astype(str).str.slice(0, 10)
    df = df.sort_values("date")

    assert (df["total_vaccinations"] > 0).all()
    df["total_vaccinations"] = df["total_vaccinations"].cumsum()

    df.loc[:, "location"] = "Czechia"
    df.loc[:, "source_url"] = url

    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[df.date >= "2021-01-17", "vaccine"] = "Moderna, Pfizer/BioNTech"

    df.to_csv("automations/output/Czechia.csv", index=False)


if __name__ == "__main__":
    main()
