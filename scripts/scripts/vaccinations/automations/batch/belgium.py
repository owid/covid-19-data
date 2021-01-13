import os
import pandas as pd


def main():

    os.system("curl -O https://covid-vaccinatie.be/api/vaccines-administered.json")

    df = pd.read_json("vaccines-administered.json")

    df = df.sort_values(["date", "cumulative"]).groupby("date", as_index=False).tail(1)
    df = df.drop(columns=["amount"])

    df.loc[:, "location"] = "Belgium"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[:, "source_url"] = "https://covid-vaccinatie.be/en"

    df.to_csv("automations/output/Belgium.csv", index=False)

    os.remove("vaccines-administered.json")


if __name__ == '__main__':
    main()
