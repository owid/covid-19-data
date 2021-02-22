import datetime
import pytz
import requests
import pandas as pd
import vaxutils


def read(source: str) -> pd.Series:
    headers = {'Content-type': 'application/json', 'Accept': '*/*'}
    json = {
        "DisaCodigo": 0,
        "IdDepartamento": "",
    }

    request = requests.post(
        source,
        json=json,
        headers=headers
    )
    request.raise_for_status()
    data = request.json()
    df = pd.DataFrame.from_records(data['Data'])
    return parse_data(df)


def parse_data(df: pd.DataFrame) -> pd.Series:
    keys = ("people_vaccinated", "total_vaccinations")
    values = (parse_people_vaccinated(df), parse_total_vaccinations(df))
    data = dict(zip(keys, values))
    return pd.Series(data=data)


def parse_total_vaccinations(df: pd.DataFrame) -> int:
    total_vaccinations = int(df.Vacunados.sum())
    return total_vaccinations


def parse_people_vaccinated(df: pd.DataFrame) -> int:
    people_vaccinated = int(df.Vacunados.sum())
    return people_vaccinated


def format_date(ds: pd.Series) -> pd.Series:
    local_time = datetime.datetime.now(pytz.timezone("America/Lima"))
    if local_time.hour < 8:
        local_time = local_time - datetime.timedelta(days=1)
    date = str(local_time.date())
    return vaxutils.enrich_data(ds, 'date', date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'location', "Peru")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'vaccine', "Sinopharm/Beijing")


def enrich_source(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'source_url', "https://gis.minsa.gob.pe/GisVisorVacunados/")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds.pipe(format_date)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://gis.minsa.gob.pe/WebApiRep2/api/Departamento/ListarVacunadosPublico"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=data['location'],
        total_vaccinations=data['total_vaccinations'],
        people_vaccinated=data['people_vaccinated'],
        people_fully_vaccinated=None,
        date=data['date'],
        source_url=data['source_url'],
        vaccine=data['vaccine']
    )


if __name__ == "__main__":
    main()
