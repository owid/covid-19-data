import pandas as pd


def main():

    df = pd.read_csv(
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vREkIbPThdAkSyX-do7JgN8T-7A4d553NZX-j21me151dggP8WY7uiNaTutRxMvOCVh2u3e7w1lxhR0/pub?gid=363514317&single=true&output=csv",
        usecols=["FECHA DE CORTE", "DOSIS APLICADAS ACUMULADAS", "SEGUNDAS DOSIS ACUMULADAS"]
    )

    df = df.rename(columns={
            "FECHA DE CORTE": "date",
            "DOSIS APLICADAS ACUMULADAS": "total_vaccinations",
            "SEGUNDAS DOSIS ACUMULADAS": "people_fully_vaccinated",
        })

    df = df.assign(
        location="Colombia",
        people_vaccinated=df.total_vaccinations - df.people_fully_vaccinated,
        source_url="https://www.minsalud.gov.co/salud/publica/Vacunacion/Paginas/Vacunacion-covid-19.aspx",
        vaccine="Pfizer/BioNTech, Sinovac",
        date=pd.to_datetime(df.date).dt.date
    )

    # Integrity checks
    assert all((df.total_vaccinations - df.total_vaccinations.shift(1)).dropna() > 0)
    assert all((df.people_fully_vaccinated - df.people_fully_vaccinated.shift(1)).dropna() > 0)

    df = df.dropna(subset=["total_vaccinations"])
    df.to_csv("automations/output/Colombia.csv", index=False)


if __name__ == "__main__":
    main()
