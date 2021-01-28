import datetime
import pandas as pd


def main():

    url = "https://info.gesundheitsministerium.gv.at/data/timeline.csv"

    df = pd.read_csv(url, sep=";")

    assert df.shape[1] == 14

    df = df[df["Name"] == "Österreich"]
    df = df[["Datum", "EingetrageneImpfungen", "Teilgeimpfte", "Vollimmunisierte"]]

    df = df.rename(columns={
        "Datum": "date",
        "EingetrageneImpfungen": "total_vaccinations",
        "Teilgeimpfte": "people_vaccinated",
        "Vollimmunisierte": "people_fully_vaccinated"
    })

    df.loc[:, "date"] = df.date.str.slice(0, 10)
    df.loc[:, "location"] = "Austria"
    df.loc[:, "source_url"] = "https://www.data.gv.at/katalog/dataset/589132b2-c000-4c60-85b4-c5036cdf3406"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"

    df.to_csv("automations/output/Austria.csv", index=False)


if __name__ == "__main__":
    main()
