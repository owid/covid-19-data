import json
import requests
import pandas as pd

def main():

    resp = requests.get("https://services3.arcgis.com/nIl76MjbPamkQiu8/arcgis/rest/services/date_wise_corona_with_positive_percentage/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=date%20asc&outSR=102100&resultOffset=0&resultRecordCount=32000&resultType=standard&cacheHint=true")
    data = json.loads(resp.text)
    data = data["features"]
    dates = [day["attributes"]["date"] for day in data]
    tested = [day["attributes"]["tested"] for day in data]
    
    df = pd.DataFrame({
        "Date": pd.to_datetime(dates, unit="ms").date,
        "Daily change in cumulative total": tested
    }).dropna()
    df = df[df["Daily change in cumulative total"] != 0]
    
    df.loc[:, "Daily change in cumulative total"] = df["Daily change in cumulative total"].astype(int)
    df.loc[:, "Country"] = "Bangladesh"
    df.loc[:, "Units"] = "tests performed"
    df.loc[:, "Testing type"] = "PCR only"
    df.loc[:, "Source label"] = "Government of Bangladesh"
    df.loc[:, "Source URL"] = "https://covid19bd.idare.io/"
    df.loc[:, "Notes"] = pd.NA

    # Manual fix for error in data
    df.loc[
        (df["Date"] == pd.to_datetime("2020-03-16")) &
        (df["Daily change in cumulative total"] == 39),
        "Date"
    ] = "2020-03-17"

    df.to_csv("automated_sheets/Bangladesh.csv", index=False)

if __name__ == "__main__":
    main()
