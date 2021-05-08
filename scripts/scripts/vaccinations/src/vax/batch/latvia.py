import os
import requests

from bs4 import BeautifulSoup
import pandas as pd


def main(paths):

    vaccine_mapping = {
        "Comirnaty": "Pfizer/BioNTech",
        "COVID-19 Vaccine Moderna": "Moderna",
        "Vaxzevria": "Oxford/AstraZeneca",
        "COVID-19 Vaccine Janssen": "Johnson&Johnson",
    }
    one_dose_vaccines = ["Johnson&Johnson"]

    url = "https://data.gov.lv/dati/eng/dataset/covid19-vakcinacijas"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    file_url = soup.find_all("a", class_="resource-url-analytics")[-1]["href"]

    df = pd.read_excel(file_url, usecols=[
        "Vakcinācijas datums", "Vakcinēto personu skaits", "Vakcinācijas posms", "Preparāts"
    ])
    df["vaccine"] = df["Preparāts"].str.strip()
    assert set(df["vaccine"].unique()) == set(vaccine_mapping.keys())
    df = df.replace(vaccine_mapping)

    # Data by vaccine
    vax = (
        df.groupby(["Vakcinācijas datums", "vaccine"], as_index=False)
        ["Vakcinēto personu skaits"].sum()
        .sort_values("Vakcinācijas datums")
        .rename(columns={
            "Vakcinācijas datums": "date", "Vakcinēto personu skaits": "total_vaccinations"}
        )
    )
    vax["total_vaccinations"] = vax.groupby("vaccine", as_index=False)["total_vaccinations"].cumsum()
    vax["location"] = "Latvia"
    vax.to_csv(paths.tmp_vax_loc_man("Latvia"), index=False)

    df = df.rename(columns={
        "Vakcinācijas datums": "date",
        "Vakcinēto personu skaits": "quantity",
        "Vakcinācijas posms": "dose_number"
    })
    df = (
        df.drop(columns=["Preparāts"])
        .groupby(["date", "dose_number", "vaccine"], as_index=False)
        .sum()
        .pivot(index=["date", "vaccine"], columns="dose_number", values="quantity")
        .reset_index()
        .sort_values("date")
    )

    df["total_vaccinations"] = df["1.pote"].fillna(0) + df["2.pote"].fillna(0)
    df = df.rename(columns={"1.pote": "people_vaccinated", "2.pote": "people_fully_vaccinated"})
    df.loc[df.vaccine.isin(one_dose_vaccines), "people_fully_vaccinated"] = df.people_vaccinated

    df = df.groupby("date", as_index=False).agg({
        "total_vaccinations": "sum",
        "people_vaccinated": "sum",
        "people_fully_vaccinated": "sum",
    })

    df[["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]] = (
        df[["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]].cumsum()
    )
   
    df.loc[:, "location"] = "Latvia"
    df.loc[:, "vaccine"] = ", ".join(vaccine_mapping.values())
    df.loc[:, "source_url"] = url

    df.to_csv(paths.tmp_vax_loc("Latvia"), index=False)


if __name__ == '__main__':
    main()
