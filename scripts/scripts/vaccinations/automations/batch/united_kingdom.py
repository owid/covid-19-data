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

    # Hard-coded point with 800k for the UK on Dec 24 2020 (Boris Johnson press conference)
    if df.loc[df["location"] == "United Kingdom", "total_vaccinations"].max() < 800000:
        df = pd.concat([df, pd.DataFrame({
            "date": "2020-12-24",
            "location": "United Kingdom",
            "total_vaccinations": [800000],
            "source_url": "https://www.gov.uk/government/speeches/prime-ministers-statement-on-eu-negotiations-24-december-2020"
        })])

    df.loc[:, "vaccine"] = "Pfizer/BioNTech"

    for loc in set(df["location"]):
        df[df["location"] == loc].to_csv(f"automations/output/{loc}.csv", index=False)

if __name__ == "__main__":
    main()
