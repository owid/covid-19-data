import json
import requests
import pandas as pd


def main():

    url = "https://w3qa5ydb4l.execute-api.eu-west-1.amazonaws.com/prod/finnishVaccinationData"
    data = json.loads(requests.get(url).content)

    df = pd.DataFrame.from_records(data)
    df = df[df["area"] == "Finland"]
    df = df.rename(columns={
        "shots": "total_vaccinations",
        "area": "location"
    })

    df["date"] = df["date"].str.slice(0, 10)

    df.loc[:, "source_url"] = "https://github.com/HS-Datadesk/koronavirus-avoindata"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"

    df.to_csv("automations/output/Finland.csv", index=False)


if __name__ == '__main__':
    main()
