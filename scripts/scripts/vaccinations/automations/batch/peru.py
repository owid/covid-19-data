import pandas as pd


def main():

    url = "https://cloud.minsa.gob.pe/s/ZgXoXqK2KLjRLxD/download"

    df = pd.read_csv(url, usecols=["FECHA_VACUNACION", "DOSIS", "FABRICANTE"])

    df["FECHA_VACUNACION"] = pd.to_datetime(df["FECHA_VACUNACION"], format="%Y%m%d").dt.date

    df = df.rename(columns={"FECHA_VACUNACION": "date", "FABRICANTE": "vaccine"})

    vaccine_mapping = {
        "SINOPHARM": "Sinopharm/Beijing",
        "PFIZER": "Pfizer/BioNTech",
    }
    assert set(df["vaccine"].unique()) == set(vaccine_mapping.keys())
    df = df.replace(vaccine_mapping)

    # vax = (
    #     df.groupby(["date", "vaccine"], as_index=False)
    #     .size()
    #     .sort_values("date")
    #     .rename(columns={"size": "total_vaccinations"})
    # )
    # vax["total_vaccinations"] = vax.groupby("vaccine", as_index=False)["total_vaccinations"].cumsum()
    # vax["location"] = "Peru"
    # vax.to_csv("automations/output/by_manufacturer/Peru.csv", index=False)

    df = (
        df.groupby(["date", "DOSIS"], as_index=False)
        .count()
        .pivot(index="date", columns="DOSIS", values="vaccine")
        .rename(columns={1: "people_vaccinated", 2: "people_fully_vaccinated"})
        .fillna(0)
        .cumsum()
        .reset_index()
    )

    df["total_vaccinations"] = df["people_vaccinated"] + df["people_fully_vaccinated"]

    df.loc[:, "location"] = "Peru"
    df.loc[:, "vaccine"] = ", ".join(sorted(vaccine_mapping.values()))
    df.loc[:, "source_url"] = "https://www.datosabiertos.gob.pe/dataset/vacunaci%C3%B3n-contra-covid-19-ministerio-de-salud-minsa"

    df.to_csv("automations/output/Peru.csv", index=False)


if __name__ == '__main__':
    main()
