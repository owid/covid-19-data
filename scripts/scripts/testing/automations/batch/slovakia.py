import requests
import json
import pandas as pd


def main():
    
    df = pd.read_csv(
        "https://github.com/Institut-Zdravotnych-Analyz/covid19-data/raw/main/OpenData_Slovakia_Covid_DailyStats.csv",
        sep=";",
        usecols=["Datum", "Dennych.testov"]
    )

    df = df.rename(columns={"Datum": "Date", "Dennych.testov": "Daily change in cumulative total"})

    df.loc[:, "Source URL"] = "https://github.com/Institut-Zdravotnych-Analyz/covid19-data"
    df.loc[:, "Source label"] = "Ministry of Health"
    df.loc[:, "Country"] = "Slovakia"
    df.loc[:, "Units"] = "tests performed"
    df.loc[:, "Testing type"] = "PCR only"
    df.loc[:, "Notes"] = pd.NA

    df.to_csv("automated_sheets/Slovakia.csv", index=False)


if __name__ == '__main__':
    main()
