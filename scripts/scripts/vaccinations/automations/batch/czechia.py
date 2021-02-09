import pandas as pd

vaccine_mapping = {
    "Comirnaty": "Pfizer/BioNTech",
    "COVID-19 Vaccine Moderna": "Moderna",
    # "AstraZeneca Custom Name (TODO)": "Oxford/AstraZeneca",
}

def main():

    source_url = "https://onemocneni-aktualne.mzcr.cz/covid-19"
    data_url = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovaci-mista.csv"

    df = pd.read_csv(data_url, parse_dates=["datum"])
    assert df.columns.tolist() == ["datum", "vakcina", "kraj_nuts_kod", "kraj_nazev", "zarizeni_kod",
       "zarizeni_nazev", "poradi_davky", "vekova_skupina"]

    # since we need to translate vaccine names, we'll check that no new
    # manufacturers were added, so that we can maintain control over this
    # IMPORTANT: If a new vaccine is added, see if it requires a single dose
    # or two doses. If it's a single-dose one, make sure to fix the calculation
    # of `total_vaccinations`
    assert set(df["vakcina"].unique()) == set(vaccine_mapping.keys())
    df = df.replace(vaccine_mapping)

    # Data by vaccine
    vax = df.groupby(["datum", "vakcina"], as_index=False).size().sort_values("datum")
    vax["size"] = vax.groupby("vakcina", as_index=False)["size"].cumsum()
    vax = vax.rename(columns={"datum": "date", "vakcina": "vaccine", "size": "total_vaccinations"})
    vax["location"] = "Czechia"
    vax.to_csv("automations/output/by_vaccine/Czechia.csv", index=False)

    df = df.groupby(["datum", "vakcina", "poradi_davky"]).size().unstack().reset_index()
    df = df.groupby("datum").agg(
        vaccine=("vakcina", lambda x: ", ".join(sorted(set(x)))),
        people_vaccinated=(1, "sum"),  # 1 means 1st dose
        people_fully_vaccinated=(2, "sum"),
    ).reset_index()

    # the following holds only because all vaccines used so far require two doses
    df["total_vaccinations"] = df["people_vaccinated"] + df["people_fully_vaccinated"]

    df = df.rename(columns={
         "datum": "date",
    })
    df["date"] = df["date"].astype(str).str.slice(0, 10)
    df = df.sort_values("date")
    assert df["date"].min() == "2020-12-27"

    for col in ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]:
        df[col] = df[col].cumsum().astype(int)

    df.loc[:, "location"] = "Czechia"
    df.loc[:, "source_url"] = source_url

    df.to_csv("automations/output/Czechia.csv", index=False)


if __name__ == "__main__":
    main()
