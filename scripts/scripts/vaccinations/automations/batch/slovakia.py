import json
import requests
import pandas as pd


def import_iza():

    # The online dashboard only starts on January 20, 2021.
    # Previous data is collected from the repo maintained by IZA:
    # https://github.com/Institut-Zdravotnych-Analyz/covid19-data
    iza = pd.read_csv(
        "https://github.com/Institut-Zdravotnych-Analyz/covid19-data/raw/main/Vaccination/OpenData_Slovakia_Vaccination_Regions.csv",
        usecols=["Date", "first_dose", "second_dose"],
        sep=";"
    )

    iza = (
        iza.groupby("Date", as_index=False)
        .sum()
        .rename(columns={
            "Date": "date",
            "first_dose": "people_vaccinated",
            "second_dose": "people_fully_vaccinated"
        })
        .sort_values("date")
    )

    iza["people_vaccinated"] = iza["people_vaccinated"].cumsum()
    iza["people_fully_vaccinated"] = iza["people_fully_vaccinated"].cumsum()
    iza["total_vaccinations"] = iza["people_vaccinated"] + iza["people_fully_vaccinated"]
    iza["people_fully_vaccinated"] = iza["people_fully_vaccinated"].replace(0, pd.NA)
    iza["source_url"] = "https://github.com/Institut-Zdravotnych-Analyz/covid19-data"

    return iza


def import_dashboard():

    url = "https://covid-19.nczisk.sk/webapi/v1/kpi?e916b653e44d1fdf57b5bbe826b624b7"
    data = json.loads(requests.get(url).content)

    df = pd.DataFrame.from_records(data["tiles"]["k31"]["data"]["d"])
    df = df[["d", "v", "v1"]].rename(columns={
        "d": "date", "v": "first_dose_only", "v1": "people_fully_vaccinated"
    })
    df["people_vaccinated"] = df["first_dose_only"] + df["people_fully_vaccinated"]
    df = df.drop(columns=["first_dose_only"])
    df["date"] = pd.to_datetime(df["date"], format="%y%m%d").dt.date.astype(str)
    df["total_vaccinations"] = df["people_vaccinated"] + df["people_fully_vaccinated"]
    df["source_url"] = "https://covid-19.nczisk.sk"

    return df


def main():

    iza = import_iza()
    dashboard = import_dashboard()

    dashboard = dashboard[dashboard["date"] > iza["date"].max()]
    df = pd.concat([iza, dashboard]).sort_values("date")

    df.loc[:, "location"] = "Slovakia"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"

    df.to_csv("automations/output/Slovakia.csv", index=False)


if __name__ == "__main__":
    main()
