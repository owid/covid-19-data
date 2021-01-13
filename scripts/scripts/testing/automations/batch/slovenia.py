import datetime
import requests
import json
import pandas as pd


def main():

    url = "https://api.sledilnik.org/api/lab-tests"
    data = json.loads(requests.get(url).content)

    dates = []
    tests = []
    cases = []

    for elem in data:
        dates.append(datetime.date(elem["year"], elem["month"], elem["day"]))
        tests.append(elem["total"]["performed"]["today"])
        cases.append(elem["total"].get("positive").get("today"))

    df = pd.DataFrame({"Date": dates, "Daily change in cumulative total": tests, "cases": cases})
    df = df.fillna(0).sort_values("Date")

    df["Positive rate"] = (
        df["cases"].rolling(7).sum() / df["Daily change in cumulative total"].rolling(7).sum()
    ).round(3)

    df = df.drop(columns=["cases"])

    df.loc[:, "Source URL"] = "https://covid-19.sledilnik.org/en/data"
    df.loc[:, "Source label"] = "National Institute of Public Health, via Sledilnik"
    df.loc[:, "Country"] = "Slovenia"
    df.loc[:, "Units"] = "tests performed"
    df.loc[:, "Notes"] = pd.NA

    df.to_csv("automated_sheets/Slovenia.csv", index=False)


if __name__ == '__main__':
    main()
