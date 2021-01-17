import requests
import pandas as pd
import vaxutils

def main():
    
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

if __name__ == "__main__":
    main()
