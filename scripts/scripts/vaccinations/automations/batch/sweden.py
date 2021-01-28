import datetime
import pandas as pd


def week_to_date(row):
    origin_date = pd.to_datetime("2019-12-29") if row.Vecka >= 52 else pd.to_datetime("2021-01-03")
    return origin_date + pd.DateOffset(days=7*int(row.Vecka))


def main():

    url = "https://fohm.maps.arcgis.com/sharing/rest/content/items/fc749115877443d29c2a49ea9eca77e9/data"
    df = pd.read_excel(url, sheet_name="Vaccinerade tidsserie")

    df = df[df["Region"] == "| Sverige |"][["Vecka", "Antal vaccinerade", "Dosnummer"]]

    df = df.pivot_table(values="Antal vaccinerade", index="Vecka", columns="Dosnummer").reset_index()

    # Week-to-date logic will stop working after 2021
    assert datetime.date.today().year < 2022

    df.loc[:, "date"] = df.apply(week_to_date, axis=1)

    df = df.drop(columns=["Vecka"]).sort_values("date").rename(columns={
        "Dos 1": "people_vaccinated", "Dos 2": "people_fully_vaccinated"
    })

    df.loc[:, "total_vaccinations"] = df["people_vaccinated"] + df["people_fully_vaccinated"]
    df.loc[:, "location"] = "Sweden"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[:, "source_url"] = "https://www.folkhalsomyndigheten.se/smittskydd-beredskap/utbrott/aktuella-utbrott/covid-19/vaccination-mot-covid-19/statistik/statistik-over-registrerade-vaccinationer-covid-19/"

    df.to_csv("automations/output/Sweden.csv", index=False)


if __name__ == '__main__':
    main()
