import requests
import pandas as pd


def main():
    
    url = "https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=vaccination_trends_data"
    data = requests.get(url).json()["vaccination_trends_data"]

    df = pd.DataFrame.from_records(data)
    df = df[df["ShortName"] == "USA"]

    df = df[["LongName", "Date", "Administered_Cumulative", "Admin_Dose_1_Cumulative", "Admin_Dose_2_Cumulative"]]
    df = df.rename(columns={
        "LongName": "location",
        "Administered_Cumulative": "total_vaccinations",
        "Admin_Dose_1_Cumulative": "people_vaccinated",
        "Admin_Dose_2_Cumulative": "people_fully_vaccinatede",
        "Date": "date"  
    })

    # Remove rows with unchanged data
    df = df.dropna().sort_values("date").groupby("total_vaccinations").head(1)

    df.loc[:, "source_url"] = "https://covid.cdc.gov/covid-data-tracker/#vaccination-trends"

    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[df["date"] >= "2020-12-23", "vaccine"] = "Moderna, Pfizer/BioNTech"

    df.to_csv("automations/output/United States.csv", index=False)


if __name__ == "__main__":
    main()
