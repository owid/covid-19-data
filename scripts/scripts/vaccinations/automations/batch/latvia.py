import requests
from bs4 import BeautifulSoup
import pandas as pd

def main():

    url = "http://www.vmnvd.gov.lv/lv/covid-19/1486-vakcinacija-pret-covid-19"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    url = soup.find(id="text1").find("a")["href"]
    url = "http://www.vmnvd.gov.lv" + url
    
    df = pd.read_excel(url, skiprows=2)
    df = df[df["Slimnīcas kods"] == "Kopā:"]
    df = df.drop(columns=["Slimnīcas kods", "Slimnīca", "Kopā"])
    df = df.melt(var_name="date")

    df["date"] = pd.to_datetime(df["date"], format="%d.%m.%Y.").dt.date.astype(str)
    df = df.sort_values("date")
    df["total_vaccinations"] = df["value"].cumsum().astype(int)
    df = df.drop(columns=["value"])

    df.loc[:, "location"] = "Latvia"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[:, "source_url"] = "http://www.vmnvd.gov.lv/lv/covid-19/1486-vakcinacija-pret-covid-19"

    df.to_csv("automations/output/Latvia.csv", index=False)

if __name__ == '__main__':
    main()
