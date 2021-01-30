import pandas as pd


def main():

    url = "https://www.data.gouv.fr/fr/datasets/r/efe23314-67c4-45d3-89a2-3faef82fae90"
    df = pd.read_csv(url, sep=";")
    assert df.shape[1] == 4

    df = df.sort_values("jour").drop(columns=["fra", "n_dose1"]).rename(columns={
        "n_cum_dose1": "people_vaccinated", "jour": "date"
    })

    df.loc[:, "total_vaccinations"] = df["people_vaccinated"]

    df.loc[:, "location"] = "France"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[:, "source_url"] = "https://www.data.gouv.fr/fr/datasets/donnees-relatives-aux-personnes-vaccinees-contre-la-covid-19-1/"

    df.to_csv("automations/output/France.csv", index=False)


if __name__ == '__main__':
    main()
