import requests
import json
import pandas as pd


def main():

    resp = requests.get("https://mapa.covid.chat/map_data")
    data = json.loads(resp.content)
    data = [(elem["date"], elem["tested_daily"]) for elem in data["chart"]]
    
    df = pd.DataFrame(data, columns=["Date", "Daily change in cumulative total"])

    df.loc[:, "Date"] = pd.to_datetime(df["Date"])
    df.loc[:, "Source URL"] = "https://www.korona.gov.sk"
    df.loc[:, "Source label"] = "National Health Information Centre"
    df.loc[:, "Country"] = "Slovakia"
    df.loc[:, "Units"] = "tests performed"
    df.loc[:, "Testing type"] = "PCR only"
    df.loc[:, "Notes"] = pd.NA

    df.to_csv("automated_sheets/Slovakia.csv", index=False)


if __name__ == '__main__':
    main()
