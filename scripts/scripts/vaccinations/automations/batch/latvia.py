import requests
from bs4 import BeautifulSoup
import pandas as pd


def main():

    url = "https://data.gov.lv/dati/eng/dataset/covid19-vakcinacijas"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    file_url = soup.find_all("a", class_="resource-url-analytics")[-1]["href"]
    
    df = pd.read_excel(file_url, skiprows=2)

    df = df[df["Vakcinācijas iestādes kods"] == "Kopā"]

    df = df.drop(columns=["Vakcinācijas iestādes kods", "Vakcinācijas iestādes nosaukums", "Kopā"])

    df = df.melt(var_name="date", value_name="daily_vaccinations")

    df = df.sort_values("date")

    df["total_vaccinations"] = df["daily_vaccinations"].cumsum().astype(int)

    df = df.drop(columns=["daily_vaccinations"])

    df.loc[:, "location"] = "Latvia"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[:, "source_url"] = url

    df.to_csv("automations/output/Latvia.csv", index=False)


if __name__ == '__main__':
    main()
