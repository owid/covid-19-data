import requests
import json
import pandas as pd


def main():
    
    df = pd.read_csv(
        "https://github.com/Institut-Zdravotnych-Analyz/covid19-data/raw/main/DailyStats/OpenData_Slovakia_Covid_DailyStats.csv",
        sep=";",
        usecols=["Datum", "Dennych.PCR.testov", "AgTests", "Dennych.PCR.prirastkov", "AgPosit"]
    )

    df = df.sort_values("Datum")
    df["Daily change in cumulative total"] = df["Dennych.PCR.testov"].fillna(0) + df["AgTests"].fillna(0)
    df["positive"] = df["Dennych.PCR.prirastkov"].fillna(0) + df["AgPosit"].fillna(0)
    df["Positive rate"] = (
        df.positive.rolling(7).mean() / df["Daily change in cumulative total"].rolling(7).mean()
    ).round(3)

    df = df[["Datum", "Daily change in cumulative total", "Positive rate"]].rename(columns={
        "Datum": "Date"
    })

    df.loc[:, "Source URL"] = "https://github.com/Institut-Zdravotnych-Analyz/covid19-data"
    df.loc[:, "Source label"] = "Ministry of Health"
    df.loc[:, "Country"] = "Slovakia"
    df.loc[:, "Units"] = "tests performed"
    df.loc[:, "Testing type"] = "PCR only"
    df.loc[:, "Notes"] = pd.NA

    df.to_csv("automated_sheets/Slovakia.csv", index=False)


if __name__ == '__main__':
    main()
