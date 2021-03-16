import datetime
import pandas as pd


def week_to_date(row):
    origin_date = pd.to_datetime("2019-12-29") if row.Vecka >= 52 else pd.to_datetime("2021-01-03")
    return origin_date + pd.DateOffset(days=7*int(row.Vecka))


def get_weekly_data():

    url = "https://fohm.maps.arcgis.com/sharing/rest/content/items/fc749115877443d29c2a49ea9eca77e9/data"
    df = pd.read_excel(url, sheet_name="Vaccinerade tidsserie")
    df = df[df["Region"] == "| Sverige |"][["Vecka", "Antal vaccinerade", "Dosnummer"]]
    df = df.pivot_table(values="Antal vaccinerade", index="Vecka", columns="Dosnummer").reset_index()

    # Week-to-date logic will stop working after 2021
    assert datetime.date.today().year < 2022
    df.loc[:, "date"] = df.apply(week_to_date, axis=1).dt.date.astype(str)

    df = df.drop(columns=["Vecka"]).sort_values("date").rename(columns={
        "Dos 1": "people_vaccinated", "Dos 2": "people_fully_vaccinated"
    })
    df.loc[:, "total_vaccinations"] = df["people_vaccinated"] + df["people_fully_vaccinated"]
    return df


def get_daily_data():

    df = pd.read_html("https://www.folkhalsomyndigheten.se/smittskydd-beredskap/utbrott/aktuella-utbrott/covid-19/vaccination-mot-covid-19/statistik/statistik-over-registrerade-vaccinationer-covid-19/")[1]

    df = df[["Datum", "Antal vaccinerademed minst 1 dos*", "Antal vaccinerademed 2 doser"]].rename(columns={
        "Datum": "date",
        "Antal vaccinerademed minst 1 dos*": "people_vaccinated",
        "Antal vaccinerademed 2 doser": "people_fully_vaccinated",
    })

    df["people_vaccinated"] = df["people_vaccinated"].str.replace(r"\s", "", regex=True).astype(int)
    df["people_fully_vaccinated"] = df["people_fully_vaccinated"].str.replace(r"\s", "", regex=True).astype(int)
    df["total_vaccinations"] = df["people_vaccinated"] + df["people_fully_vaccinated"]
    return df


def main():

    daily = get_daily_data()
    weekly = get_weekly_data()
    weekly = weekly[weekly["date"] < daily["date"].min()]
    df = pd.concat([daily, weekly]).sort_values("date").reset_index(drop=True)

    df.loc[:, "location"] = "Sweden"
    df.loc[:, "vaccine"] = "Oxford/AstraZeneca, Pfizer/BioNTech"
    df.loc[:, "source_url"] = "https://www.folkhalsomyndigheten.se/smittskydd-beredskap/utbrott/aktuella-utbrott/covid-19/vaccination-mot-covid-19/statistik/statistik-over-registrerade-vaccinationer-covid-19/"

    df.to_csv("automations/output/Sweden.csv", index=False)


if __name__ == '__main__':
    main()
