import requests
from bs4 import BeautifulSoup
import pandas as pd

def main():

    url = "https://data.gov.lv/dati/eng/dataset/covid19-vakcinacijas"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    file_url = soup.find_all("a", class_="resource-url-analytics")[-1]["href"]
    
    df = pd.read_excel(file_url, usecols=["Vakcinācijas datums", "Vakcinēto personu skaits"])

    df = df.rename(columns={
        "Vakcinācijas datums": "date",
        "Vakcinēto personu skaits": "total_vaccinations"
    })

    df = df.groupby("date", as_index=False).sum()
    df = df.sort_values("date")
    df["total_vaccinations"] = df["total_vaccinations"].cumsum()

    df.loc[:, "location"] = "Latvia"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[:, "source_url"] = url

    df.to_csv("automations/output/Latvia.csv", index=False)

if __name__ == '__main__':
    main()
