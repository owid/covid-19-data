import datetime
import requests
import json
import pandas as pd


def main():

    url = "https://api.sledilnik.org/api/lab-tests"
    data = json.loads(requests.get(url).content)

    dates = []
    pcr = []
    ag = []
    positive_pcr = []
    positive_ag = []

    for elem in data:
        dates.append(datetime.date(elem["year"], elem["month"], elem["day"]))
        pcr.append(elem["total"]["performed"]["today"])
        ag.append(elem["data"]["hagt"]["performed"].get("today"))
        positive_pcr.append(elem["total"].get("positive").get("today"))
        positive_ag.append(elem["data"]["hagt"]["positive"].get("today"))

    df = pd.DataFrame({
        "Date": dates,
        "pcr": pcr, 
        "ag": ag, 
        "positive_pcr": positive_pcr,
        "positive_ag": positive_ag,
    })
    df = df.fillna(0).sort_values("Date")

    # In February 2021, the governemnt started using PCR to confirm all antigen tests.
    # For all intents and purposes Slovenia's case definition is therefore now PCR only,
    # even if some people (and we don't know how many) are getting screened and found via antigen
    # before they are confirmed with PCR. It makes sense to stick to PCR only for the
    # positivity rate — which is what the government has done in its own communication.
    df["Daily change in cumulative total"] = df.pcr
    df["cases"] = df.positive_pcr

    df["Positive rate"] = (
        df["cases"].rolling(7).sum() / df["Daily change in cumulative total"].rolling(7).sum()
    ).round(3)

    df = df[["Date", "Daily change in cumulative total", "Positive rate"]]

    df.loc[:, "Source URL"] = "https://covid-19.sledilnik.org/en/data"
    df.loc[:, "Source label"] = "National Institute of Public Health, via Sledilnik"
    df.loc[:, "Country"] = "Slovenia"
    df.loc[:, "Units"] = "tests performed"
    df.loc[:, "Notes"] = pd.NA

    df = df[df["Daily change in cumulative total"] != 0]

    df.to_csv("automated_sheets/Slovenia.csv", index=False)


if __name__ == '__main__':
    main()
