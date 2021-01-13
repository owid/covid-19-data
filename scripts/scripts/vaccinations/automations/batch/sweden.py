import requests
import pandas as pd
from bs4 import BeautifulSoup


def week_to_date(row):
    assert row["year"] <= 2021

    if row["year"] == 2020:
        origin = pd.to_datetime("2019-12-29")
    elif row["year"] == 2021:
        origin = pd.to_datetime("2021-01-03")

    offset = pd.DateOffset(days=row["week"]*7)
    return origin + offset


def main():

    url = "https://www.folkhalsomyndigheten.se/smittskydd-beredskap/utbrott/aktuella-utbrott/covid-19/vaccination-mot-covid-19/statistik-over-vaccinerade-mot-covid-19/"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    table = soup.find("div", id="content-primary").find("table")
    df = pd.read_html(str(table))[0]

    df = df[df["Vecka, år"] != "Totalt"]

    df[["week", "year"]] = df["Vecka, år"].str.split(", ", expand=True).astype(int)
    df["date"] = df.apply(week_to_date, axis=1)
    df = df.sort_values("date")

    df["weekly_vaccinations"] = df["Förbrukat"].str.replace(r"\s", "", regex=True).astype(int)
    df["total_vaccinations"] = df["weekly_vaccinations"].cumsum()

    df = df[["date", "total_vaccinations"]]

    df.loc[:, "location"] = "Sweden"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[:, "source_url"] = url

    df.to_csv("automations/output/Sweden.csv", index=False)


if __name__ == "__main__":
    main()
