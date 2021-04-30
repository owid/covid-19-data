import pandas as pd


def main():

    url = "https://github.com/jmcastagnetto/covid-19-peru-vacunas/raw/main/datos/vacunas_covid_resumen.csv"

    df = pd.read_csv(url, usecols=["fecha_vacunacion", "fabricante", "dosis", "n_reg"])

    df = df.rename(columns={"fecha_vacunacion": "date", "fabricante": "vaccine"})

    vaccine_mapping = {
        "SINOPHARM": "Sinopharm/Beijing",
        "PFIZER": "Pfizer/BioNTech",
    }
    assert set(df["vaccine"].unique()) == set(vaccine_mapping.keys()), "New vaccine found!"
    df = df.replace(vaccine_mapping)

    df = (
        df
        .drop(columns="vaccine")
        .groupby(["date", "dosis"], as_index=False)
        .sum()
        .pivot(index="date", columns="dosis", values="n_reg")
        .rename(columns={1: "people_vaccinated", 2: "people_fully_vaccinated"})
        .fillna(0)
        .sort_values("date")
        .cumsum()
        .reset_index()
    )

    df["total_vaccinations"] = df["people_vaccinated"] + df["people_fully_vaccinated"]

    df.loc[:, "location"] = "Peru"
    df.loc[:, "vaccine"] = ", ".join(sorted(vaccine_mapping.values()))
    df.loc[:, "source_url"] = (
        "https://www.datosabiertos.gob.pe/dataset/vacunaci%C3%B3n-contra-covid-19-ministerio-de-salud-minsa"
    )

    df.to_csv("output/Peru.csv", index=False)


if __name__ == '__main__':
    main()
