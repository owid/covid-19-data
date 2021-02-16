import json
import requests
import vaxutils
import pandas as pd


def read(source: str) -> pd.Series:
    data = json.loads(requests.get(source).content)["features"][0]["attributes"]
    return parse_data(data)


def parse_data(data: pd.DataFrame) -> pd.Series:
    data = {'date': parse_date(data), 'total_vaccinations': parse_total_vaccinations(data),
            'people_vaccinated': parse_people_vaccinated(data),
            'people_fully_vaccinated': parse_people_fully_vaccinated(data)}
    return pd.Series(data=data)


def parse_date(data: pd.DataFrame) -> str:
    date = data["Data"]
    date = str(pd.to_datetime(date, unit="ms").date())
    return date


def parse_total_vaccinations(data: pd.DataFrame) -> str:
    total_vaccinations = data["Vacinados_Ac"]
    return total_vaccinations


def parse_people_fully_vaccinated(data: pd.DataFrame) -> str:
    people_fully_vaccinated = data["Inoculacao2_Ac"]
    return people_fully_vaccinated


def parse_people_vaccinated(data: pd.DataFrame) -> str:
    people_vaccinated = data["Inoculacao1_Ac"]
    return people_vaccinated


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "Portugal")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine', "Moderna, Pfizer/BioNTech")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'source_url', "https://covid19.min-saude.pt/ponto-de-situacao-atual-em-portugal/")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://services5.arcgis.com/eoFbezv6KiXqcnKq/arcgis/rest/services/Covid19_Total_Vacinados/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&resultOffset=0&resultRecordCount=50&resultType=standard&cacheHint=true"
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
