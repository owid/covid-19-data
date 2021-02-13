import datetime
import json
import pytz
import requests
import pandas as pd
import vaxutils


def main():

    url = "https://services-eu1.arcgis.com/zk7YlClTgerl62BY/arcgis/rest/services/global_szczepienia_widok3/FeatureServer/0/query?f=json&where=Data%20BETWEEN%20(CURRENT_TIMESTAMP%20-%20INTERVAL%20%2724%27%20HOUR)%20AND%20CURRENT_TIMESTAMP&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&resultOffset=0&resultRecordCount=1&resultType=standard"
    data = json.loads(requests.get(url).content)

    total_vaccinations = data["features"][0]["attributes"]["SZCZEPIENIA_SUMA"]
    people_fully_vaccinated = data["features"][0]["attributes"]["DAWKA_2_SUMA"]
    people_vaccinated = total_vaccinations - people_fully_vaccinated

    date = data["features"][0]["attributes"]["Data"]
    date = str((pd.to_datetime(date, unit="ms").date() - pd.DateOffset(days=1)).date())

    vaxutils.increment(
        location="Poland",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url="https://www.gov.pl/web/szczepimysie/raport-szczepien-przeciwko-covid-19",
        vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
