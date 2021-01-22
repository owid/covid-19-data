import pandas as pd


def main():

    url = "https://epistat.sciensano.be/Data/COVID19BE_VACC.csv"
    df = pd.read_csv(url, usecols=["Date", "PartlyVaccinated", "FullyVaccinated"])

    df = df.rename(columns={
        "Date": "date",
        "PartlyVaccinated": "people_vaccinated",
        "FullyVaccinated": "people_fully_vaccinated"
    })

    df = df.groupby("date", as_index=False).sum().sort_values("date")
    df["people_vaccinated"] = df["people_vaccinated"].cumsum()
    df["people_fully_vaccinated"] = df["people_fully_vaccinated"].cumsum()
    df["total_vaccinations"] = df["people_vaccinated"] + df["people_fully_vaccinated"]

    df.loc[:, "location"] = "Belgium"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[:, "source_url"] = "https://datastudio.google.com/embed/u/0/reporting/c14a5cfc-cab7-4812-848c-0369173148ab/page/hOMwB"

    df.to_csv("automations/output/Belgium.csv", index=False)


if __name__ == '__main__':
    main()
