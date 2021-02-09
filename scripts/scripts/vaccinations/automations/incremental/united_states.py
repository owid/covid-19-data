import requests
from glob import glob
import pandas as pd
import vaxutils


def get_country_data():

    url = "https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=vaccination_data"
    data = requests.get(url).json()
    data = data["vaccination_data"]

    for d in data:
        if d["ShortName"] == "USA":
            data = d
            break

    total_vaccinations = data["Doses_Administered"]
    people_vaccinated = data["Administered_Dose1"]
    people_fully_vaccinated = data["Administered_Dose2"]

    date = data["Date"]
    try:
        date = pd.to_datetime(date, format="%m/%d/%Y")
    except:
        date = pd.to_datetime(date, format="%Y-%m-%d")
    date = str(date.date())

    vaxutils.increment(
        location="United States",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url="https://covid.cdc.gov/covid-data-tracker/#vaccinations",
        vaccine="Moderna, Pfizer/BioNTech"
    )


def get_vaccine_data():
    dfs = []
    for file in glob("us_states/input/cdc_data_*.csv"):
        try:
            df = pd.read_csv(file, usecols=[
                "Date", "LongName", "Administered_Pfizer", "Administered_Moderna"
            ])
            dfs.append(df)
        except:
            pass
    df = pd.concat(dfs)
    df = df[df.LongName == "United States"].sort_values("Date").rename(columns={
        "Date": "date",
        "LongName": "location",
        "Administered_Pfizer": "Pfizer/BioNTech",
        "Administered_Moderna": "Moderna",
    })
    df = df.melt(["date", "location"], var_name="vaccine", value_name="total_vaccinations")
    df.to_csv("automations/output/by_manufacturer/United States.csv", index=False)


if __name__ == "__main__":
    get_country_data()
    get_vaccine_data()
