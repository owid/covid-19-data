import pandas as pd

def main():

    df = pd.read_csv(
        "https://raw.githubusercontent.com/sledilnik/data/master/csv/vaccination.csv",
        usecols=["date", "vaccination.administered.todate", "vaccination.administered2nd.todate"]
    )

    df = df.rename(columns={
        "vaccination.administered.todate": "people_vaccinated",
        "vaccination.administered2nd.todate": "people_fully_vaccinated"
    })

    df = df[-df["people_vaccinated"].isna()]
    df["total_vaccinations"] = df["people_vaccinated"] + df["people_fully_vaccinated"].fillna(0)

    df.loc[:, "location"] = "Slovenia"
    df.loc[:, "source_url"] = "https://covid-19.sledilnik.org/en/stats"
    df.loc[:, "vaccine"] = "Oxford/AstraZeneca, Pfizer/BioNTech"

    df.to_csv("automations/output/Slovenia.csv", index=False)

if __name__ == "__main__":
    main()
