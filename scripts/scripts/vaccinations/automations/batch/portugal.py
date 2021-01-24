import json
import requests
import pandas as pd


def main():

    url = "https://services5.arcgis.com/eoFbezv6KiXqcnKq/arcgis/rest/services/Covid19_Total_Vacinados/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&resultOffset=0&resultRecordCount=50&resultType=standard&cacheHint=true"
    data = json.loads(requests.get(url).content)

    df = pd.DataFrame.from_records([elem["attributes"] for elem in data["features"]])

    df = df[["Data", "Inoculacao1_Ac", "Inoculacao2_Ac", "Vacinados_Ac"]].rename(columns={
        "Data": "date",
        "Inoculacao1_Ac": "people_vaccinated",
        "Inoculacao2_Ac": "people_fully_vaccinated",
        "Vacinados_Ac": "total_vaccinations",
    })

    df["date"] = pd.to_datetime(df["date"], unit="ms")

    df.loc[:, "location"] = "Portugal"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[:, "source_url"] = "https://covid19.min-saude.pt/ponto-de-situacao-atual-em-portugal/"

    df.to_csv("automations/output/Portugal.csv", index=False)



if __name__ == "__main__":
    main()
