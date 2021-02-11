import pandas as pd
import vaxutils
import datetime
import pytz

date = None


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source, usecols=["primera_dosis_cantidad", "segunda_dosis_cantidad"])


def add_totals(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        people_vaccinated=int(input["primera_dosis_cantidad"].sum()),
        people_fully_vaccinated=int(input["segunda_dosis_cantidad"].sum()),
    ).assign(
        total_vaccinations=lambda df: df.people_vaccinated + df.people_fully_vaccinated,
    )


def format_date(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(date=str(datetime.datetime.now(pytz.timezone("America/Argentina/Buenos_Aires")).date()))


def enrich_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        location="Argentina",
        vaccine="Sputnik V",
        source_url="http://datos.salud.gob.ar/dataset/vacunas-contra-covid-19-dosis-aplicadas-en-la-republica-argentina",
    )


def pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(add_totals)
            .pipe(format_date)
            .pipe(enrich_columns)
    )


def main():
    source = "https://sisa.msal.gov.ar/datos/descargas/covid-19/files/Covid19VacunasAgrupadas.csv"
    data = read(source).pipe(pipeline)

    vaxutils.increment(
        location=str(data['location'].values[0]),
        total_vaccinations=int(data['total_vaccinations'].values[0]),
        people_vaccinated=int(data['people_vaccinated'].values[0]),
        people_fully_vaccinated=int(data['people_fully_vaccinated'].values[0]),
        date=str(data['date'].values[0]),
        source_url=str(data['source_url'].values[0]),
        vaccine=str(data['vaccine'].values[0])
    )


if __name__ == "__main__":
    main()
