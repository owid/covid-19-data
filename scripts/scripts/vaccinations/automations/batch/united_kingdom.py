import pandas as pd

def main():

    uk = pd.read_csv("https://api.coronavirus.data.gov.uk/v2/data?areaType=overview&metric=cumPeopleReceivingFirstDose&metric=cumPeopleReceivingSecondDose&format=csv")
    subnational = pd.read_csv("https://api.coronavirus.data.gov.uk/v2/data?areaType=nation&metric=cumPeopleReceivingFirstDose&metric=cumPeopleReceivingSecondDose&format=csv")

    df = pd.concat([uk, subnational])

    df = df.rename(columns={
        "areaName": "location",
        "cumPeopleReceivingFirstDose": "total_vaccinations"
    })

    df = df[["date", "location", "total_vaccinations"]]
    df.loc[:, "source_url"] = "https://coronavirus.data.gov.uk/"

    # Hard-coded data point
    if df.loc[df["location"] == "United Kingdom", "total_vaccinations"].max() < 1000000:
        df = pd.concat([df, pd.DataFrame({
            "date": "2021-01-01",
            "location": "United Kingdom",
            "total_vaccinations": [1000000],
            "source_url": "https://twitter.com/MattHancock/status/1344987404487294976"
        })])

    df.loc[:, "vaccine"] = "Pfizer/BioNTech"

    for loc in set(df["location"]):
        df[df["location"] == loc].to_csv(f"automations/output/{loc}.csv", index=False)

if __name__ == "__main__":
    main()
