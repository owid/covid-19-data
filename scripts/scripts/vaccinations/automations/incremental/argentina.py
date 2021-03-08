import pandas as pd
import vaxutils
import datetime
import pytz


def read(source: str) -> pd.Series:
    df = pd.read_csv(source, usecols=["primera_dosis_cantidad", "segunda_dosis_cantidad"])
    return df.sum(level=0, axis=1).sum()


def translate_index(input: pd.Series) -> pd.Series:
    return input.rename({
        'primera_dosis_cantidad': 'people_vaccinated',
        'segunda_dosis_cantidad': 'people_fully_vaccinated',
    })


def add_totals(input: pd.Series) -> pd.Series:
    total_vaccinations = input['people_vaccinated'] + input['people_fully_vaccinated']
    return vaxutils.enrich_data(input, 'total_vaccinations', total_vaccinations)


def format_date(input: pd.Series) -> pd.Series:
    local_time = datetime.datetime.now(pytz.timezone("America/Argentina/Buenos_Aires"))
    if local_time.hour < 8:
        local_time = local_time - datetime.timedelta(days=1)
    date = str(local_time.date())
    return vaxutils.enrich_data(input, 'date', date)


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "Argentina")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine', "Oxford/AstraZeneca, Sinopharm/Beijing, Sputnik V")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'source_url',
                                "http://datos.salud.gob.ar/dataset/vacunas-contra-covid-19-dosis-aplicadas-en-la-republica-argentina")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(translate_index)
            .pipe(add_totals)
            .pipe(format_date)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://sisa.msal.gov.ar/datos/descargas/covid-19/files/Covid19VacunasAgrupadas.csv"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=str(data['location']),
        total_vaccinations=int(data['total_vaccinations']),
        people_vaccinated=int(data['people_vaccinated']),
        people_fully_vaccinated=int(data['people_fully_vaccinated']),
        date=str(data['date']),
        source_url=str(data['source_url']),
        vaccine=str(data['vaccine'])
    )


if __name__ == "__main__":
    main()
