
import datetime
import json
import requests
import pandas as pd
import vaxutils


def main():

    url = "https://services-eu1.arcgis.com/z6bHNio59iTqqSUY/arcgis/rest/services/Covid19_Vaccine_Administration_Hosted_View/FeatureServer/0/query?f=json&where=1%3D1&outFields=*&returnGeometry=false&outStatistics=%5B%7B%22onStatisticField%22%3A%22firstDose%22%2C%22outStatisticFieldName%22%3A%22firstDose_max%22%2C%22statisticType%22%3A%22max%22%7D%5D"
    data = json.loads(requests.get(url).content)
    people_vaccinated = data["features"][0]["attributes"]["firstDose_max"]

    url = "https://services-eu1.arcgis.com/z6bHNio59iTqqSUY/arcgis/rest/services/Covid19_Vaccine_Administration_Hosted_View/FeatureServer/0/query?f=json&where=1%3D1&outFields=*&returnGeometry=false&outStatistics=%5B%7B%22onStatisticField%22%3A%22secondDose%22%2C%22outStatisticFieldName%22%3A%22secondDose_max%22%2C%22statisticType%22%3A%22max%22%7D%5D"
    data = json.loads(requests.get(url).content)
    people_fully_vaccinated = data["features"][0]["attributes"]["secondDose_max"]

    total_vaccinations = people_vaccinated + people_fully_vaccinated

    url = "https://services-eu1.arcgis.com/z6bHNio59iTqqSUY/arcgis/rest/services/Covid19_Vaccine_Administration_Hosted_View/FeatureServer/0/query?f=json&where=1%3D1&outFields=*&returnGeometry=false&outStatistics=%5B%7B%22onStatisticField%22%3A%22relDate%22%2C%22outStatisticFieldName%22%3A%22relDate_max%22%2C%22statisticType%22%3A%22max%22%7D%5D"
    data = json.loads(requests.get(url).content)
    date = data["features"][0]["attributes"]["relDate_max"]
    date = str(pd.to_datetime(date, unit="ms").date())

    vaxutils.increment(
        location="Ireland",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url="https://covid19ireland-geohive.hub.arcgis.com/",
        vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
