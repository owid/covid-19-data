import json
import requests
import vaxutils
import pandas as pd

firstDose_source = "https://services-eu1.arcgis.com/z6bHNio59iTqqSUY/arcgis/rest/services/Covid19_Vaccine_Administration_Hosted_View/FeatureServer/0/query?f=json&where=1%3D1&outFields=*&returnGeometry=false&outStatistics=%5B%7B%22onStatisticField%22%3A%22firstDose%22%2C%22outStatisticFieldName%22%3A%22firstDose_max%22%2C%22statisticType%22%3A%22max%22%7D%5D"
secondDose_source = "https://services-eu1.arcgis.com/z6bHNio59iTqqSUY/arcgis/rest/services/Covid19_Vaccine_Administration_Hosted_View/FeatureServer/0/query?f=json&where=1%3D1&outFields=*&returnGeometry=false&outStatistics=%5B%7B%22onStatisticField%22%3A%22secondDose%22%2C%22outStatisticFieldName%22%3A%22secondDose_max%22%2C%22statisticType%22%3A%22max%22%7D%5D"
date_source = "https://services-eu1.arcgis.com/z6bHNio59iTqqSUY/arcgis/rest/services/Covid19_Vaccine_Administration_Hosted_View/FeatureServer/0/query?f=json&where=1%3D1&outFields=*&returnGeometry=false&outStatistics=%5B%7B%22onStatisticField%22%3A%22relDate%22%2C%22outStatisticFieldName%22%3A%22relDate_max%22%2C%22statisticType%22%3A%22max%22%7D%5D"


def read() -> pd.Series:
    return parse_data()


def parse_data() -> pd.Series:
    keys = ("date", "people_vaccinated", "people_fully_vaccinated")
    values = (parse_date(), parse_people_vaccinated(), parse_people_fully_vaccinated())
    data = dict(zip(keys, values))
    return pd.Series(data=data)


def parse_date() -> str:
    data = json.loads(requests.get(date_source).content)["features"][0]["attributes"]
    date = data["relDate_max"]
    date = str(pd.to_datetime(date, unit="ms").date())
    return date


def parse_people_vaccinated() -> int:
    data = json.loads(requests.get(firstDose_source).content)["features"][0]["attributes"]
    people_vaccinated = int(data["firstDose_max"])
    return people_vaccinated


def parse_people_fully_vaccinated() -> int:
    data = json.loads(requests.get(secondDose_source).content)["features"][0]["attributes"]
    people_fully_vaccinated = int(data["secondDose_max"])
    return people_fully_vaccinated


def add_totals(input: pd.Series) -> pd.Series:
    total_vaccinations = input['people_vaccinated'] + input['people_fully_vaccinated']
    return vaxutils.enrich_data(input, 'total_vaccinations', total_vaccinations)


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "Ireland")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine', "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'source_url', "https://covid19ireland-geohive.hub.arcgis.com/")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(add_totals)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    data = read().pipe(pipeline)
    vaxutils.increment(
        location=data['location'],
        total_vaccinations=data['total_vaccinations'],
        people_vaccinated=data['people_vaccinated'],
        people_fully_vaccinated=data['people_fully_vaccinated'],
        date=data['date'],
        source_url=data['source_url'],
        vaccine=data['vaccine']
    )


if __name__ == "__main__":
    main()
