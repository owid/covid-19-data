import pandas as pd


def main():
     
    url = "https://impfdashboard.de/static/data/germany_vaccinations_timeseries_v2.tsv"

    df = pd.read_csv(url, sep="\t", usecols=[
        "date", "dosen_kumulativ", "personen_erst_kumulativ", "personen_voll_kumulativ",
        "dosen_biontech_kumulativ", "dosen_moderna_kumulativ", "dosen_astrazeneca_kumulativ"
    ])

    df = df.rename(columns={
        "dosen_kumulativ": "total_vaccinations",
        "personen_erst_kumulativ": "people_vaccinated",
        "personen_voll_kumulativ": "people_fully_vaccinated",
        "dosen_biontech_kumulativ": "Pfizer/BioNTech",
        "dosen_moderna_kumulativ": "Moderna",
        "dosen_astrazeneca_kumulativ": "Oxford/AstraZeneca",
    })

    # Data by vaccine
    vax = df[["date", "Pfizer/BioNTech", "Moderna", "Oxford/AstraZeneca"]]
    vax = vax.melt("date", var_name="vaccine", value_name="total_vaccinations")
    vax["location"] = "Germany"
    vax.to_csv("automations/output/by_manufacturer/Germany.csv", index=False)

    # Country data
    df = df[["date", "total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]]

    df.loc[:, "location"] = "Germany"
    df.loc[:, "source_url"] = "https://impfdashboard.de/"

    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[df["date"] >= "2021-01-12", "vaccine"] = "Moderna, Pfizer/BioNTech"
    df.loc[df["date"] >= "2021-02-08", "vaccine"] = "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"

    df.to_csv("automations/output/Germany.csv", index=False)


if __name__ == "__main__":
    main()
