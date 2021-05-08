import os
import pandas as pd


def main(paths):

    vaccine_mapping = {
        "Pfizer/BioNTech": "Pfizer/BioNTech",
        "Moderna": "Moderna",
        "Vaxzevria (AstraZeneca)": "Oxford/AstraZeneca",
        "Janssen": "Johnson&Johnson",
    }
    one_dose_vaccines = ["Johnson&Johnson"]

    url = (
        "https://raw.githubusercontent.com/italia/covid19-opendata-vaccini/master/dati/"
        "somministrazioni-vaccini-latest.csv"
    )
    df = pd.read_csv(url, usecols=["data_somministrazione", "fornitore", "prima_dose", "seconda_dose"])
    assert set(df["fornitore"].unique()) == set(vaccine_mapping.keys())
    df = df.replace(vaccine_mapping)
    df["total_vaccinations"] = df["prima_dose"] + df["seconda_dose"]
    df = df.rename(columns={"data_somministrazione": "date", "fornitore": "vaccine"})

    # Data by manufacturer
    by_manufacturer = (
        df.groupby(["date", "vaccine"], as_index=False)["total_vaccinations"]
        .sum()
        .sort_values("date")
    )
    by_manufacturer["total_vaccinations"] = by_manufacturer.groupby("vaccine")["total_vaccinations"].cumsum()
    by_manufacturer["location"] = "Italy"
    by_manufacturer.to_csv(paths.out_tmp_man("Italy"), index=False)

    # Vaccination data
    df = df.rename(columns={
        "prima_dose": "people_vaccinated",
        "seconda_dose": "people_fully_vaccinated",
    })
    df.loc[df.vaccine.isin(one_dose_vaccines), "people_fully_vaccinated"] = df.people_vaccinated
    df = (
        df.groupby("date", as_index=False)[["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]]
        .sum()
        .sort_values("date")
    )

    df[["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]] = (
        df[["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]].cumsum()
    )

    df.loc[:, "location"] = "Italy"
    df.loc[:, "source_url"] = url
    df.loc[:, "vaccine"] = ", ".join(sorted(vaccine_mapping.values()))

    df.to_csv(paths.out_tmp("Italy"), index=False)


if __name__ == "__main__":
    main()
