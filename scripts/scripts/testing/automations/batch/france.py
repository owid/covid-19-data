import os
import datetime
import json
import pandas as pd
import numpy as np


def tests_performed():

    # PCR tests
    assert datetime.datetime.utcfromtimestamp(os.stat("input/data.xlsx").st_mtime).date() == datetime.date.today(), \
        "File is too old. Import new data from Geodes."
    pcr = pd.read_excel("input/data.xlsx", sheet_name ="Graphiques", skiprows=3)
    pcr = pcr.iloc[0:pcr[pcr.Indicateurs.isna()].index.min(), :]
    pcr["Valeur"] = pcr["Valeur"].str.replace(r"\s", "").astype(int)
    pcr = pcr.rename(columns={"Valeur": "PCR", "Indicateurs": "Date"})

    # Antigen tests
    antigen = pd.read_csv("https://www.data.gouv.fr/fr/datasets/r/a4db9eea-9423-4fcd-8373-e2b1e3f27ad3", sep=";")
    antigen = antigen[antigen["act_description"].isin(["FORFAIT COVID  AVEC PRÉLÈVEMENT", "FORFAIT COVID  SANS PRÉLÈVEMENT"])]
    assert len(antigen) > 0
    antigen = antigen[["date", "quantity"]].groupby("date", as_index=False).sum()
    antigen = antigen.rename(columns={"quantity": "Antigen", "date": "Date"})

    df = pd.merge(pcr, antigen, on="Date", how="outer").fillna(0)
    df.loc[:, "Daily change in cumulative total"] = df["PCR"].add(df["Antigen"]).astype(int)
    import pdb; pdb.set_trace()
    df = df.drop(columns=["PCR", "Antigen"])

    df.loc[:, "Country"] = "France"
    df.loc[:, "Units"] = "tests performed"
    df.loc[:, "Testing type"] = "includes non-PCR"
    df.loc[:, "Cumulative total"] = pd.NA
    df.loc[:, "Source URL"] = "https://geodes.santepubliquefrance.fr / https://www.data.gouv.fr/fr/datasets/covid-executions-et-ventes-de-tests-antigeniques-rapides-en-pharmacie/"
    df.loc[:, "Source label"] = "National Public Health Agency / IQVIA France"
    df.loc[:, "Notes"] = pd.NA

    df.to_csv("automated_sheets/France - tests performed.csv", index=False)


def people_tested():

    url = "https://www.data.gouv.fr/fr/datasets/r/dd0de5d9-b5a5-4503-930a-7b08dc0adc7c"
    df = pd.read_csv(url, sep=";", usecols=["jour", "cl_age90", "T"])

    df = (
        df[df.cl_age90 == 0]
        .rename(columns={"jour": "Date", "T": "Daily change in cumulative total"})
        .drop(columns=["cl_age90"])
    )

    df.loc[:, "Country"] = "France"
    df.loc[:, "Units"] = "people tested"
    df.loc[:, "Testing type"] = "PCR only"
    df.loc[:, "Cumulative total"] = pd.NA
    df.loc[:, "Source URL"] = "https://www.data.gouv.fr/fr/datasets/donnees-relatives-aux-resultats-des-tests-virologiques-covid-19/"
    df.loc[:, "Source label"] = "National Public Health Agency"
    df.loc[:, "Notes"] = pd.NA

    df.to_csv("automated_sheets/France - people tested.csv", index=False)


if __name__ == "__main__":
    tests_performed()
    people_tested()
