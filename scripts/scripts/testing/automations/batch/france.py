import os
import datetime
import json
import pandas as pd
import numpy as np


def main():

    # People tested
    url = "https://www.data.gouv.fr/fr/datasets/r/dd0de5d9-b5a5-4503-930a-7b08dc0adc7c"
    df = pd.read_csv(url, sep=";", usecols=["jour", "cl_age90", "T"])

    df = (
        df[df.cl_age90 == 0]
        .rename(columns={"jour": "Date", "T": "Daily change in cumulative total"})
        .drop(columns=["cl_age90"])
    )

    # Positive rate
    pr = pd.read_csv(
        "https://www.data.gouv.fr/fr/datasets/r/381a9472-ce83-407d-9a64-1b8c23af83df",
        usecols=["extract_date", "tx_pos"]
    )
    pr.loc[:, "tx_pos"] = pr["tx_pos"].div(100).round(3)
    pr = pr.rename(columns={"extract_date": "Date", "tx_pos": "Positive rate"})

    df = pd.merge(df, pr, on="Date", how="outer").sort_values("Date")
    df = df.dropna(subset=["Daily change in cumulative total", "Positive rate"], how="all")

    df.loc[:, "Country"] = "France"
    df.loc[:, "Units"] = "people tested"
    df.loc[:, "Cumulative total"] = pd.NA
    df.loc[:, "Source URL"] = "https://www.data.gouv.fr/fr/datasets/donnees-relatives-aux-resultats-des-tests-virologiques-covid-19/"
    df.loc[:, "Source label"] = "National Public Health Agency"
    df.loc[:, "Notes"] = pd.NA

    df.to_csv("automated_sheets/France.csv", index=False)


if __name__ == "__main__":
    main()
