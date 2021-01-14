import datetime
import json
import pytz
import requests
import vaxutils


def main():

    url = "https://services-eu1.arcgis.com/z6bHNio59iTqqSUY/arcgis/rest/services/Covid19_Vaccine_Administration_Data/FeatureServer/0/query?f=json&where=1%3D1&outFields=*&returnGeometry=false&outStatistics=%5B%7B%22onStatisticField%22%3A%22total_number_of_1st_dose_admini%22%2C%22outStatisticFieldName%22%3A%22total_number_of_1st_dose_admini_max%22%2C%22statisticType%22%3A%22max%22%7D%5D"
    data = json.loads(requests.get(url).content)

    count = data["features"][0]["attributes"]["total_number_of_1st_dose_admini_max"]

    date = str(datetime.datetime.now(pytz.timezone("Europe/Dublin")).date())

    vaxutils.increment(
        location="Ireland",
        total_vaccinations=count,
        date=date,
        source_url="https://covid19ireland-geohive.hub.arcgis.com/",
        vaccine="Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
