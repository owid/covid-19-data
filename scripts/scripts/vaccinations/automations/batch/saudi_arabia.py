import json
import requests
import pandas as pd


def main():

    url = "https://coronamap.sa/Home/GetVaccineCountryInfo?countryname=Saudi%20Arabia"
    data = json.loads(requests.get(url).content)

    df = pd.DataFrame.from_records(data["features"])

    df.loc[:, "location"] = "Saudi Arabia"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[df.date >= "2021-02-18", "vaccine"] = "Oxford/AstraZeneca, Pfizer/BioNTech"
    df.loc[:, "source_url"] = "https://coronamap.sa"

    df = df[df.total_vaccinations > 0]

    df.to_csv("automations/output/Saudi Arabia.csv", index=False)


if __name__ == '__main__':
    main()
