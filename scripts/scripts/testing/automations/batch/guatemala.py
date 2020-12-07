"""Constructs daily time series of COVID-19 testing data for Guatemala.

Official dashboard: https://tablerocovid.mspas.gob.gt/."""

import datetime
import pandas as pd

COUNTRY = "Guatemala"
UNITS = "people tested"
SOURCE_LABEL = "Ministry of Health and Social Assistance"
SOURCE_URL = "https://gtmvigilanciacovid.shinyapps.io/3869aac0fb95d6baf2c80f19f2da5f98/_w_ea02fb72/session/46e8a816c9b4d1127e90f38398f94af9/download/tamizadosFER?w=ea02fb72"

def main() -> None:
    df = pd.read_csv(SOURCE_URL)
    df = df.filter(regex=r"^20[\d-]{8}$")
    df = pd.melt(df).groupby("variable", as_index=False).sum()
    df = df[df["value"] != 0]
    df = df.rename(columns={"variable": "Date", "value": "Daily change in cumulative total"})
    df.loc[:, "Country"] = COUNTRY
    df.loc[:, "Units"] = UNITS
    df.loc[:, "Source label"] = SOURCE_LABEL
    df.loc[:, "Source URL"] = SOURCE_URL
    df.loc[:, "Notes"] = pd.NA
    df.to_csv("automated_sheets/Guatemala.csv", index=False)
    assert pd.to_datetime(df.Date.max()) > (datetime.date.today() - datetime.timedelta(days=7))
    return None

if __name__ == "__main__":
    main()
