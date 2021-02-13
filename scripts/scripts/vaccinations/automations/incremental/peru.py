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
        "IdDistrito": "",
        "IdEstablecimiento": 0,
        "IdFase": 1,
        "IdProvincia": "",
    }

    request = requests.post(
        "https://gis.minsa.gob.pe/WebApiProd/api/ActVacunasResumen/ResumenFasePublico",
        json=json,
        headers=headers
    )
    request.raise_for_status()

    data = request.json()

    df = pd.DataFrame(data['Data'][0]['Vacunas'])

    people_vaccinated = int(df[df['NroDosis'] == 1]['Vacunados'].values[0])
    people_fully_vaccinated = int(df[df['NroDosis'] == 2]['Vacunados'].values[0])
    total_vaccinations = people_vaccinated + people_fully_vaccinated

    local_time = datetime.datetime.now(pytz.timezone("America/Lima"))
    if local_time.hour < 8:
        local_time = local_time - datetime.timedelta(days=1)
    date = str(local_time.date())

    vaxutils.increment(
        location="Peru",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url="https://gis.minsa.gob.pe/GisVisorVacunados/",
        vaccine="Sinopharm/Beijing"
    )


if __name__ == '__main__':
    main()
