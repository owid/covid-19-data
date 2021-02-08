import datetime
import json
import pytz
import requests
import vaxutils


def main():

    url = "https://services5.arcgis.com/eoFbezv6KiXqcnKq/arcgis/rest/services/Covid19_Total_Vacinados/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&resultOffset=0&resultRecordCount=50&resultType=standard&cacheHint=true"
    data = json.loads(requests.get(url).content)

    total_vaccinations = data["features"][0]["attributes"]["Vacinados_Ac"]
    people_vaccinated = data["features"][0]["attributes"]["Inoculacao1_Ac"]
    people_fully_vaccinated = data["features"][0]["attributes"]["Inoculacao2_Ac"]

    date = str(datetime.datetime.now(pytz.timezone("Europe/Lisbon")).date())

    vaxutils.increment(
        location="Portugal",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url="https://covid19.min-saude.pt/ponto-de-situacao-atual-em-portugal/",
        vaccine="Moderna, Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
