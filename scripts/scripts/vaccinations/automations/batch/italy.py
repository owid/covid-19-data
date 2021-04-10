import pandas as pd


def get_country_data():
    df = pd.read_csv(
        "https://raw.githubusercontent.com/italia/covid19-opendata-vaccini/master/dati/somministrazioni-vaccini-summary-latest.csv",
        usecols=["data_somministrazione", "area", "totale", "prima_dose", "seconda_dose"]
    )

    df = df[df["area"] != "ITA"]
    df = df.sort_values("data_somministrazione").groupby("data_somministrazione", as_index=False).sum()

    df.loc[:, "totale"] = df["totale"].cumsum()
    df.loc[:, "prima_dose"] = df["prima_dose"].cumsum()
    df.loc[:, "seconda_dose"] = df["seconda_dose"].cumsum()

    df = df.rename(columns={
        "data_somministrazione": "date",
        "totale": "total_vaccinations",
        "prima_dose": "people_vaccinated",
        "seconda_dose": "people_fully_vaccinated"
    })
    df = df[df["date"] >= "2020-01-01"]

    df.loc[:, "location"] = "Italy"
    df.loc[:, "source_url"] = "https://github.com/italia/covid19-opendata-vaccini/blob/master/dati/somministrazioni-vaccini-summary-latest.csv"

    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[df["date"] >= "2021-01-14", "vaccine"] = "Moderna, Pfizer/BioNTech"
    df.loc[df["date"] >= "2021-02-06", "vaccine"] = "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"

    df.to_csv("automations/output/Italy.csv", index=False)


def get_vaccine_data():

    vaccine_mapping = {
        "Pfizer/BioNTech": "Pfizer/BioNTech",
        "Moderna": "Moderna",
        "Vaxzevria (AstraZeneca)": "Oxford/AstraZeneca",
    }
    url = "https://raw.githubusercontent.com/italia/covid19-opendata-vaccini/master/dati/somministrazioni-vaccini-latest.csv"
    df = pd.read_csv(url, usecols=["data_somministrazione", "fornitore", "prima_dose", "seconda_dose"])
    assert set(df["fornitore"].unique()) == set(vaccine_mapping.keys())
    df = df.replace(vaccine_mapping)
    df["total_vaccinations"] = df["prima_dose"] + df["seconda_dose"]
    df = (
        df.groupby(["data_somministrazione", "fornitore"], as_index=False)["total_vaccinations"]
        .sum()
        .rename(columns={"data_somministrazione": "date", "fornitore": "vaccine"})
        .sort_values("date")
    )
    df["total_vaccinations"] = df.groupby("vaccine")["total_vaccinations"].cumsum()
    df["location"] = "Italy"
    df.to_csv("automations/output/by_manufacturer/Italy.csv", index=False)


if __name__ == "__main__":
    get_country_data()
    get_vaccine_data()
