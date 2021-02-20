import datetime
import requests
import pandas as pd
import pytz
import vaxutils


def main():

    headers = {'Content-type': 'application/json', 'Accept': '*/*'}
    json = {
        "DisaCodigo": 0,
        "IdDepartamento": "",
    }

    request = requests.post(
        "https://gis.minsa.gob.pe/WebApiRep2/api/Departamento/ListarVacunadosPublico",
        json=json,
        headers=headers
    )
    request.raise_for_status()

    data = request.json()

    df = pd.DataFrame.from_records(data['Data'])

    total_vaccinations = int(df.Vacunados.sum())
    people_vaccinated = total_vaccinations

    local_time = datetime.datetime.now(pytz.timezone("America/Lima"))
    if local_time.hour < 8:
        local_time = local_time - datetime.timedelta(days=1)
    date = str(local_time.date())

    vaxutils.increment(
        location="Peru",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=None,
        date=date,
        source_url="https://gis.minsa.gob.pe/GisVisorVacunados/",
        vaccine="Sinopharm/Beijing"
    )


if __name__ == '__main__':
    main()
