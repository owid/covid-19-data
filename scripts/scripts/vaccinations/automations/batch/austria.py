import pandas as pd


def main():

    url = "https://info.gesundheitsministerium.gv.at/data/national.csv"

    df = pd.read_csv(url, sep=";", usecols=["Datum", "Impfungen"])
    df = df.rename(columns={"Datum": "date", "Impfungen": "total_vaccinations"})

    df = df[(df["total_vaccinations"].notnull()) & (df["total_vaccinations"] > 0)]
    df = df.groupby("total_vaccinations", as_index=False).min()

    df.loc[:, "location"] = "Austria"
    df.loc[:, "source_url"] = "https://datadashboard.health.gov.il/COVID-19/general"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"

    df.to_csv("automations/output/Austria.csv", index=False)


if __name__ == "__main__":
    main()
