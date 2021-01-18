import datetime
import json
import requests
import pandas as pd


def main():

    url = "https://datadashboardapi.health.gov.il/api/queries/vaccinated"

    data = json.loads(requests.get(url).content)

    df = pd.DataFrame.from_records(data)

    df = df.rename(columns={
        "Day_Date": "date",
        "vaccinated_cum": "people_vaccinated",
        "vaccinated_seconde_dose_cum": "people_fully_vaccinated"
    })

    df = df.groupby(["people_vaccinated", "people_fully_vaccinated"], as_index=False).min()

    df["total_vaccinations"] = df["people_vaccinated"].add(df["people_fully_vaccinated"])

    df["date"] = df["date"].str.slice(0, 10)

    df = df[["date", "total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]]

    df.loc[:, "location"] = "Israel"
    df.loc[:, "source_url"] = "https://datadashboard.health.gov.il/COVID-19/general"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"

    if datetime.datetime.now().hour < 12:
        df = df[df["date"] <= str(datetime.date.today())]

    df.to_csv("automations/output/Israel.csv", index=False)


if __name__ == "__main__":
    main()
