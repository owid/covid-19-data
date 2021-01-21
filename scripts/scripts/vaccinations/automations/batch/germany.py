import pandas as pd


def main():
     
    url = "https://impfdashboard.de/static/data/germany_vaccinations_timeseries_v2.tsv"

    df = pd.read_csv(url, sep="\t", usecols=[
        "date", "dosen_kumulativ", "personen_erst_kumulativ", "personen_voll_kumulativ"
    ])
    df = df.rename(columns={
        "dosen_kumulativ": "total_vaccinations",
        "personen_erst_kumulativ": "people_vaccinated",
        "personen_voll_kumulativ": "people_fully_vaccinated"
    })

    df.loc[:, "location"] = "Germany"
    df.loc[:, "source_url"] = "https://impfdashboard.de/"

    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[df["date"] >= "2021-01-12", "vaccine"] = "Moderna, Pfizer/BioNTech"

    df.to_csv("automations/output/Germany.csv", index=False)


if __name__ == "__main__":
    main()
