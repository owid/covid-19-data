import json
import requests
import pandas as pd

def main():

    url = "https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/vaccine_administration_timeseries_canada.csv"
    df = pd.read_csv(url, usecols=["date_vaccine_administered", "cumulative_avaccine"])

    df = df.rename(columns={
        "date_vaccine_administered": "date",
        "cumulative_avaccine": "total_vaccinations"
    })

    df["date"] = pd.to_datetime(df["date"], format="%d-%m-%Y")
    
    df.loc[:, "location"] = "Canada"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[:, "source_url"] = "https://github.com/ishaberry/Covid19Canada/blob/master/timeseries_canada/vaccine_administration_timeseries_canada.csv"

    df.to_csv("automations/output/Canada.csv", index=False)

if __name__ == "__main__":
    main()
