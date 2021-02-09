import json
import requests
import pandas as pd


def main():

    DATA_URL = 'https://services3.arcgis.com/MF53hRPmwfLccHCj/ArcGIS/rest/services/COVID_vakcinavimas_chart_name/FeatureServer/0/query'
    PARAMS = {
        'f': 'json',
        'where': "municipality_code='00' AND vaccinated>0",
        'returnGeometry': False,
        'spatialRel': 'esriSpatialRelIntersects',
        'outFields': 'date,vaccine_name,dose_number,vaccinated',
        'resultOffset': 0,
        'resultRecordCount': 32000,
        'resultType': 'standard'
    }
    res = requests.get(DATA_URL, params=PARAMS)
    
    data = [elem["attributes"] for elem in json.loads(res.content)["features"]]

    df = pd.DataFrame.from_records(data)

    df["date"] = pd.to_datetime(df["date"], unit="ms")

    df = (
        df
        .groupby(["date", "dose_number"], as_index=False)
        .sum()
        .pivot(index="date", columns="dose_number", values="vaccinated")
        .fillna(0)
        .reset_index()
        .rename(columns={"Pirma dozė": "people_vaccinated", "Antra dozė": "people_fully_vaccinated"})
        .sort_values("date")
    )

    df["people_vaccinated"] = df["people_vaccinated"].cumsum()
    df["people_fully_vaccinated"] = df["people_fully_vaccinated"].cumsum()
    df["total_vaccinations"] = df["people_vaccinated"] + df["people_fully_vaccinated"]
    df = df.replace(0, pd.NA)

    df.loc[:, "location"] = "Lithuania"
    df.loc[:, "source_url"] = "https://ls-osp-sdg.maps.arcgis.com/apps/opsdashboard/index.html#/b7063ad3f8c149d394be7f043dfce460"

    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[df["date"] >= "2021-01-13", "vaccine"] = "Moderna, Pfizer/BioNTech"
    df.loc[df["date"] >= "2021-02-07", "vaccine"] = "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"

    df.to_csv("automations/output/Lithuania.csv", index=False)


if __name__ == "__main__":
    main()
