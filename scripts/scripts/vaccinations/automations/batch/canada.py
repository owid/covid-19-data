import pandas as pd


def get_coverage():

    url = "https://health-infobase.canada.ca/src/data/covidLive/vaccination-coverage-map.csv"
    df = pd.read_csv(url, usecols=["week_end", "numtotal_atleast1dose", "numtotal_2doses", "prename"])
    df = df[df["prename"] == "Canada"]
    df = df.rename(columns={
        "prename": "location",
        "week_end": "date",
        "numtotal_atleast1dose": "people_vaccinated",
        "numtotal_2doses": "people_fully_vaccinated",
    })
    df[["people_vaccinated", "people_fully_vaccinated"]] = df[["people_vaccinated", "people_fully_vaccinated"]].astype(int)
    return df


def get_doses():

    url = "https://health-infobase.canada.ca/src/data/covidLive/vaccination-administration.csv"
    df = pd.read_csv(url, usecols=["report_date", "numtotal_all_administered", "prename"])
    df = df[df["prename"] == "Canada"]
    df = df.rename(columns={
        "prename": "location",
        "report_date": "date",
        "numtotal_all_administered": "total_vaccinations",
    })
    return df


def main():
    df = (
        pd.merge(get_coverage(), get_doses(), on=["location", "date"], how="outer")
        .reset_index(drop=True)
        .sort_values("date")
    )
    df.loc[df["people_fully_vaccinated"] == 0, "total_vaccinations"] = df["people_vaccinated"]

    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[df["date"] >= "2020-12-31", "vaccine"] = "Moderna, Pfizer/BioNTech"
    df.loc[:, "source_url"] = "https://health-infobase.canada.ca/covid-19/vaccination-coverage/"

    df.to_csv("automations/output/Canada.csv", index=False)


if __name__ == "__main__":
    main()
