import pandas as pd

def main():

    uk = pd.read_csv("https://api.coronavirus.data.gov.uk/v2/data?areaType=overview&metric=cumPeopleReceivingFirstDose&metric=cumPeopleReceivingSecondDose&format=csv")
    subnational = pd.read_csv("https://api.coronavirus.data.gov.uk/v2/data?areaType=nation&metric=cumPeopleReceivingFirstDose&metric=cumPeopleReceivingSecondDose&format=csv")

    df = pd.concat([uk, subnational])

    df = df.rename(columns={"areaName": "location"})

    df["total_vaccinations"] = df["cumPeopleReceivingFirstDose"] + df["cumPeopleReceivingSecondDose"]

    df = df.drop(columns=["cumPeopleReceivingFirstDose", "cumPeopleReceivingSecondDose"])

    df = df[["date", "location", "total_vaccinations"]]
    df.loc[:, "source_url"] = "https://coronavirus.data.gov.uk/"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[df["date"] >= "2021-01-04", "vaccine"] = "Oxford/AstraZeneca, Pfizer/BioNTech"

    for loc in set(df["location"]):
        df[df["location"] == loc].to_csv(f"automations/output/{loc}.csv", index=False)

if __name__ == "__main__":
    main()
