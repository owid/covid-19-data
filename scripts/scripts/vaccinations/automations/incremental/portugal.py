import json
import requests
import vaxutils
import pandas as pd


def read(source: str) -> pd.Series:
    data = json.loads(requests.get(source).content)["features"][0]["attributes"]
    return parse_data(data)


def parse_data(data: pd.DataFrame) -> pd.Series:
    keys = ("date", "total_vaccinations", "people_vaccinated", "people_fully_vaccinated")
    values = (parse_date(data), parse_total_vaccinations(data), parse_people_vaccinated(data),
              parse_people_fully_vaccinated(data))
    data = dict(zip(keys, values))
    return pd.Series(data=data)


def parse_date(data: dict) -> str:
    date = data["Data"]
    date = str(pd.to_datetime(date, unit="ms").date())
    return date


def parse_total_vaccinations(data: dict) -> int:
    return int(data["Vacinados_Ac"])


def parse_people_fully_vaccinated(data: dict) -> int:
    return int(data["Inoculacao2_Ac"])


def parse_people_vaccinated(data: dict) -> int:
    return int(data["Inoculacao1_Ac"])


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
