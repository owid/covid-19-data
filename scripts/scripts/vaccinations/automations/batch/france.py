import pandas as pd


def main():

    url = "https://www.data.gouv.fr/fr/datasets/r/96db2c1a-8c0c-413c-9a07-f6f62f3d4daf"
    df = pd.read_csv(url, usecols=["jour", "n_dose1", "n_dose2", "sexe"], sep=";")

    df = df[df["sexe"] == 0].drop(columns="sexe")

    df = df.groupby("jour", as_index=False).sum().sort_values("jour").rename(columns={
        "n_dose1": "people_vaccinated", "n_dose2": "people_fully_vaccinated", "jour": "date"
    })

    df.loc[:, "people_vaccinated"] = df["people_vaccinated"].cumsum()
    df.loc[:, "people_fully_vaccinated"] = df["people_fully_vaccinated"].cumsum()
    df.loc[:, "total_vaccinations"] = df["people_vaccinated"] + df["people_fully_vaccinated"]

    df.loc[:, "location"] = "France"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[:, "source_url"] = "https://www.data.gouv.fr/fr/datasets/donnees-relatives-aux-personnes-vaccinees-contre-la-covid-19-1/"

    df.to_csv("automations/output/France.csv", index=False)


if __name__ == '__main__':
    main()
