import pandas as pd


def main():
     
    source_url = "https://onemocneni-aktualne.mzcr.cz/covid-19"
    data_url = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani.csv"

    df = pd.read_csv(data_url, parse_dates=["datum"])
    assert df.columns.tolist() == ["datum", "vakcina", "kraj_nuts_kod", "kraj_nazev", "vekova_skupina",
       "prvnich_davek", "druhych_davek", "celkem_davek"]

    # since we need to translate vaccine names, we'll check that no new
    # manufacturers were added, so that we can maintain control over this
    assert set(df["vakcina"].unique()) == {"Comirnaty", "COVID-19 Vaccine Moderna"}
    df = df.replace({
        "Comirnaty": "Pfizer/BioNTech",
        "COVID-19 Vaccine Moderna": "Moderna",
    })

    df = df.groupby("datum").agg(
        vaccine=("vakcina", lambda x: ", ".join(sorted(list(set(x))))),
        total_vaccinations=("celkem_davek", "sum"),
        people_vaccinated=("prvnich_davek", "sum"),
        people_fully_vaccinated=("druhych_davek", "sum"),
    ).reset_index()

    df = df.rename(columns={
         "datum": "date",
    })
    df["date"] = df["date"].astype(str).str.slice(0, 10)
    df = df.sort_values("date")

    for col in ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]:
        df[col] = df[col].cumsum()

    df.loc[:, "location"] = "Czechia"
    df.loc[:, "source_url"] = source_url
    df.to_csv("automations/output/Czechia.csv", index=False)


if __name__ == "__main__":
    main()
