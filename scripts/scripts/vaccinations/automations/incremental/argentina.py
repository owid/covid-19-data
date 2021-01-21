import datetime
import pandas as pd
import pytz
import vaxutils


def main():

    url = "https://sisa.msal.gov.ar/datos/descargas/covid-19/files/Covid19VacunasAgrupadas.csv"
    df = pd.read_csv(url, usecols=["primera_dosis_cantidad", "segunda_dosis_cantidad"])

    people_vaccinated = int(df["primera_dosis_cantidad"].sum())
    people_fully_vaccinated = int(df["segunda_dosis_cantidad"].sum())
    total_vaccinations = people_vaccinated + people_fully_vaccinated

    date = str(datetime.datetime.now(pytz.timezone("America/Argentina/Buenos_Aires")).date())

    vaxutils.increment(
        location="Argentina",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url="http://datos.salud.gob.ar/dataset/vacunas-contra-covid-19-dosis-aplicadas-en-la-republica-argentina",
        vaccine="Sputnik V"
    )


if __name__ == '__main__':
    main()
