import pandas as pd

vaccine_mapping = {
    "Comirnaty": "Pfizer/BioNTech",
    "COVID-19 Vaccine Moderna": "Moderna",
    # "AstraZeneca Custom Name (TODO)": "Oxford/AstraZeneca",
}

def main():

    source_url = "https://onemocneni-aktualne.mzcr.cz/covid-19"
    data_url = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani.csv"

    df = pd.read_csv(data_url, parse_dates=["datum"])
    assert df.columns.tolist() == ["datum", "vakcina", "kraj_nuts_kod", "kraj_nazev", "vekova_skupina",
       "prvnich_davek", "druhych_davek", "celkem_davek"]

    # since we need to translate vaccine names, we'll check that no new
    # manufacturers were added, so that we can maintain control over this
    # IMPORTANT: If a new vaccine is added, see if it requires a single dose
    # or two doses. If it's a single-dose one, make sure to fix the calculation
    # of `people_fully_vaccinated` and an assertion about basic arithmetics
    assert set(df["vakcina"].unique()) == set(vaccine_mapping.keys())
    df = df.replace(vaccine_mapping)

    df = df.groupby("datum").agg(
        vaccine=("vakcina", lambda x: ", ".join(sorted(set(x)))),
        total_vaccinations=("celkem_davek", "sum"),
        people_vaccinated=("prvnich_davek", "sum"),
        # the following holds only because all vaccines used so far require two doses
        people_fully_vaccinated=("druhych_davek", "sum"),
    ).reset_index()

    # this holds as long as all vaccines require two doses
    assert (df["total_vaccinations"] == df["people_vaccinated"] + df["people_fully_vaccinated"]).all()

    df = df.rename(columns={
         "datum": "date",
    })
    df["date"] = df["date"].astype(str).str.slice(0, 10)
    df = df.sort_values("date")
    assert df["date"].min() == "2020-12-27"

    for col in ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]:
        df[col] = df[col].cumsum()

    df.loc[:, "location"] = "Czechia"
    df.loc[:, "source_url"] = source_url

    df.to_csv("automations/output/Czechia.csv", index=False)


if __name__ == "__main__":
    main()
