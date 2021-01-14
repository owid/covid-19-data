import datetime
import json
import pytz
import requests
import vaxutils


def main():

    url = "https://services5.arcgis.com/eoFbezv6KiXqcnKq/arcgis/rest/services/Covid19_Total_Vacinados/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&resultOffset=0&resultRecordCount=50&resultType=standard&cacheHint=true"
    data = json.loads(requests.get(url).content)

    count = data["features"][0]["attributes"]["Vacinados_Ac"]

    date = str(datetime.datetime.now(pytz.timezone("Europe/Lisbon")).date())

    vaxutils.increment(
        location="Portugal",
        total_vaccinations=count,
        date=date,
        source_url="https://covid19.min-saude.pt/ponto-de-situacao-atual-em-portugal/",
        vaccine="Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
