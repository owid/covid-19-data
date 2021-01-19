import pandas as pd


def main():

    url = "https://github.com/bmesuere/covid/raw/master/data/vaccines_cumulative.csv"
    df = pd.read_csv(url)

    df = df[df["REGION"] == "Belgium"].rename(columns={
        "DATE": "date", "REGION": "location", "VACCINES_TOTAL": "total_vaccinations"
    })

    df = df.groupby(["location", "total_vaccinations"], as_index=False).min()

    df["people_vaccinated"] = df["total_vaccinations"]
    df["people_fully_vaccinated"] = pd.NA

    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[:, "source_url"] = "https://datastudio.google.com/embed/u/0/reporting/c14a5cfc-cab7-4812-848c-0369173148ab/page/hOMwB"

    df.to_csv("automations/output/Belgium.csv", index=False)


if __name__ == '__main__':
    main()
