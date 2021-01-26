import json
import requests
import pandas as pd


def main():

    url = "https://services5.arcgis.com/eoFbezv6KiXqcnKq/arcgis/rest/services/Covid19_Amostras/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=Data_do_Relatorio%20asc&resultOffset=0&resultRecordCount=32000&resultType=standard&cacheHint=true"

    data = json.loads(requests.get(url).content)
    data = [elem["attributes"] for elem in data["features"]]

    df = pd.DataFrame.from_records(data)
    df = df[["Total_Amostras__Ac", "Data_do_Relatorio"]].rename(columns={
        "Data_do_Relatorio": "Date", "Total_Amostras__Ac": "Cumulative total"
    })

    df["Date"] = pd.to_datetime(df["Date"], unit="ms")

    df["Country"] = "Portugal"
    df["Units"] = "tests performed"
    df["Source URL"] = "https://covid19.min-saude.pt/ponto-de-situacao-atual-em-portugal/"
    df["Source label"] = "Ministry of Health"
    df["Notes"] = pd.NA
    df["Testing type"] = "includes non-PCR"

    df.to_csv("automated_sheets/Portugal.csv", index=False)


if __name__ == '__main__':
    main()
