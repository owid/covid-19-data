import pandas as pd


def main():

    df = pd.read_csv(
        "https://raw.githubusercontent.com/Institut-Zdravotnych-Analyz/covid19-data/main/OpenData_Slovakia_Vaccination_Regions.csv",
        usecols=["Date", "first_dose", "second_dose"],
        sep=";"
    )

    df = (
        df
        .groupby("Date", as_index=False)
        .sum()
        .rename(columns={
            "Date": "date", "first_dose": "people_vaccinated", "second_dose": "people_fully_vaccinated"
        })
        .sort_values("date")
    )

    df["people_vaccinated"] = df["people_vaccinated"].cumsum()
    df["people_fully_vaccinated"] = df["people_fully_vaccinated"].cumsum()
    df["total_vaccinations"] = df["people_vaccinated"] + df["people_fully_vaccinated"]
    df["people_fully_vaccinated"] = df["people_fully_vaccinated"].replace(0, pd.NA)

    df.loc[:, "location"] = "Slovakia"
    df.loc[:, "source_url"] = "https://github.com/Institut-Zdravotnych-Analyz/covid19-data/blob/main/OpenData_Slovakia_Vaccination_Regions.csv"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"

    df.to_csv("automations/output/Slovakia.csv", index=False)


if __name__ == "__main__":
    main()
