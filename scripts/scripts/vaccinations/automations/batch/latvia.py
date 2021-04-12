import requests
from bs4 import BeautifulSoup
import pandas as pd


def main():

    url = "https://data.gov.lv/dati/eng/dataset/covid19-vakcinacijas"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    file_url = soup.find_all("a", class_="resource-url-analytics")[-1]["href"]
    
    df = pd.read_excel(file_url, usecols=[
        "Vakcinācijas datums", "Vakcinēto personu skaits", "Vakcinācijas posms", "Preparāts"
    ])

    vaccine_mapping = {
        "Comirnaty": "Pfizer/BioNTech",
        "COVID-19 Vaccine Moderna": "Moderna",
        "Vaxzevria": "Oxford/AstraZeneca",
    }
    df["Preparāts"] = df["Preparāts"].str.strip()
    assert set(df["Preparāts"].unique()) == set(vaccine_mapping.keys())
    df = df.replace(vaccine_mapping)

    # Data by vaccine
    vax = (
        df.groupby(["Vakcinācijas datums", "Preparāts"], as_index=False)
        ["Vakcinēto personu skaits"].sum()
        .sort_values("Vakcinācijas datums")
        .rename(columns={"Vakcinācijas datums": "date", "Preparāts": "vaccine", "Vakcinēto personu skaits": "total_vaccinations"})
    )
    vax["total_vaccinations"] = vax.groupby("vaccine", as_index=False)["total_vaccinations"].cumsum()
    vax["location"] = "Latvia"
    vax.to_csv("automations/output/by_manufacturer/Latvia.csv", index=False)

    df = df.rename(columns={
        "Vakcinācijas datums": "date",
        "Vakcinēto personu skaits": "quantity",
        "Vakcinācijas posms": "dose_number"
    })

    df = (
        df.groupby(["date", "dose_number"], as_index=False)
        .sum()
        .pivot(index="date", columns="dose_number", values="quantity")
        .reset_index()
        .sort_values("date")
    )

    df["people_vaccinated"] = df["1.pote"].cumsum()
    df["people_fully_vaccinated"] = df["2.pote"].cumsum()
    df["total_vaccinations"] = df["people_vaccinated"] + df["people_fully_vaccinated"]

    df = df.drop(columns=["1.pote", "2.pote"])

    df.loc[:, "location"] = "Latvia"
    df.loc[:, "vaccine"] = "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
    df.loc[:, "source_url"] = url

    df.to_csv("automations/output/Latvia.csv", index=False)


if __name__ == '__main__':
    main()
