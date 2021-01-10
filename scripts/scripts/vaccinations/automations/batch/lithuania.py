import json
import requests
import pandas as pd


def main():

    DATA_URL = 'https://services3.arcgis.com/MF53hRPmwfLccHCj/ArcGIS/rest/services/COVID_vakcinacijos_chart/FeatureServer/0/query'
    PARAMS = {
        'f': 'json',
        'where': "vaccinated>0",
        'returnGeometry': False,
        'spatialRel': 'esriSpatialRelIntersects',
        'outFields': 'date,vaccinated,municipality_name',
        'resultOffset': 0,
        'resultRecordCount': 32000,
        'resultType': 'standard'
    }
    res = requests.get(DATA_URL, params=PARAMS)
    
    data = [elem["attributes"] for elem in json.loads(res.content)["features"]]

    df = pd.DataFrame.from_records(data)
    df = df[df["municipality_name"] == "Lietuva"]

    df["date"] = pd.to_datetime(df["date"], unit="ms")

    df = df.sort_values("date")
    df["total_vaccinations"] = df["vaccinated"].cumsum()

    df = df.drop(columns=["vaccinated", "municipality_name"])

    df.loc[:, "location"] = "Lithuania"
    df.loc[:, "source_url"] = "https://ls-osp-sdg.maps.arcgis.com/apps/opsdashboard/index.html#/b7063ad3f8c149d394be7f043dfce460"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"

    df.to_csv("automations/output/Lithuania.csv", index=False)


if __name__ == "__main__":
    main()
