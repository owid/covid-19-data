import os
import pandas as pd


def main():

    url = "https://covid-vaccinatie.be/api/v1/administered.csv"
    os.system(f"curl -O {url} -s")

    df = pd.read_csv("administered.csv", usecols=["date", "first_dose", "second_dose"])

    df = df.rename(columns={
        "first_dose": "people_vaccinated",
        "second_dose": "people_fully_vaccinated"
    })

    df = df.groupby("date", as_index=False).sum().sort_values("date")
    df["people_vaccinated"] = df["people_vaccinated"].cumsum()
    df["people_fully_vaccinated"] = df["people_fully_vaccinated"].cumsum()
    df["total_vaccinations"] = df["people_vaccinated"] + df["people_fully_vaccinated"]

    df.loc[:, "location"] = "Belgium"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[df["date"] >= "2021-01-17", "vaccine"] = "Moderna, Pfizer/BioNTech"
    df.loc[df["date"] >= "2021-02-08", "vaccine"] = "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
    df.loc[:, "source_url"] = "https://covid-vaccinatie.be/en"

    df.to_csv("automations/output/Belgium.csv", index=False)

    os.remove("administered.csv")


if __name__ == '__main__':
    main()
