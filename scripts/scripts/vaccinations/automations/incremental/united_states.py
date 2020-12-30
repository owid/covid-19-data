import json
import requests
import pandas as pd
import vax_utils

def main():
    
    url = "https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=vaccination_data"
    data = requests.get(url).json()
    data = data["vaccination_data"][0]

    count = data["Doses_Administered"]

    date = data["Date"]
    # date = pd.to_datetime(date, format="%m/%d/%Y")
    # date = str(date.date())

    vax_utils.increment(
        location="United States",
        total_vaccinations=count,
        date=date,
        source_url="https://covid.cdc.gov/covid-data-tracker/#vaccinations",
        vaccine="Moderna, Pfizer/BioNTech"
    )

if __name__ == "__main__":
    main()
