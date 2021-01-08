import pandas as pd

def main():

    df = pd.read_csv(
        "https://github.com/italia/covid19-opendata-vaccini/raw/master/dati/somministrazioni-vaccini-summary-latest.csv",
        usecols=["data_somministrazione", "area", "totale"]
    )

    df = df[df["area"] == "ITA"]
    df = df.sort_values("data_somministrazione")
    df.loc[:, "totale"] = df["totale"].cumsum()

    df = df.rename(columns={"data_somministrazione": "date", "totale": "total_vaccinations"})
    df = df.drop(columns=["area"])
    df = df[df["date"] >= "2020-01-01"]

    df.loc[:, "location"] = "Italy"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[:, "source_url"] = "https://github.com/italia/covid19-opendata-vaccini/blob/master/dati/somministrazioni-vaccini-summary-latest.csv"

    df.to_csv("automations/output/Italy.csv", index=False)

if __name__ == "__main__":
    main()
