"""constructs a daily time series for Finland of the daily change in COVID-19 tests.
API documentation: https://thl.fi/fi/tilastot-ja-data/aineistot-ja-palvelut/avoin-data/varmistetut-koronatapaukset-suomessa-covid-19-
"""

import json
import requests
import pandas as pd

def main():
    url = "https://services7.arcgis.com/nuPvVz1HGGfa0Eh7/arcgis/rest/services/korona_testimaara_paivittain/FeatureServer/0/query?f=json&where=date%3Etimestamp%20%272020-02-25%2022%3A59%3A59%27&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=OBJECTID%2Ctestimaara_kumulatiivinen%2Cdate&orderByFields=date%20asc&resultOffset=0&resultRecordCount=4000&resultType=standard&cacheHint=true"

    # retrieves data
    res = requests.get(url)
    assert res.ok

    # extract data
    data = json.loads(res.content)["features"]
    dates = [d.get("attributes").get("date") for d in data]
    dates = pd.to_datetime(dates, unit="ms").date
    total_tests = [d.get("attributes").get("testimaara_kumulatiivinen") for d in data]
    
    # build dataframe
    df = pd.DataFrame({"Date": dates, "Cumulative total": total_tests})
    df = df.groupby("Cumulative total", as_index=False).min()

    df.loc[:, "Country"] = "Finland"
    df.loc[:, "Units"] = "tests performed"
    df.loc[:, "Source URL"] = "https://experience.arcgis.com/experience/d40b2aaf08be4b9c8ec38de30b714f26"
    df.loc[:, "Source label"] = "Finnish Department of Health and Welfare"
    df.loc[:, "Notes"] = pd.NA

    df.to_csv("automated_sheets/Finland.csv", index=False)

if __name__ == "__main__":
    main()
