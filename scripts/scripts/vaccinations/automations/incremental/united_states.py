import json
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

    count = data["Doses_Administered"]

    date = data["Date"]
    # date = pd.to_datetime(date, format="%m/%d/%Y")
    # date = str(date.date())

    vaxutils.increment(
        location="United States",
        total_vaccinations=count,
        date=date,
        source_url="https://covid.cdc.gov/covid-data-tracker/#vaccinations",
        vaccine="Moderna, Pfizer/BioNTech"
    )

if __name__ == "__main__":
    main()
