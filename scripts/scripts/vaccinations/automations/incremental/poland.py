import json
import requests
import vaxutils
import pandas as pd


def read(source: str) -> pd.Series:
    data = json.loads(requests.get(source).content)["features"][0]["attributes"]
    return parse_data(data)


def parse_data(data: dict) -> pd.Series:
    keys = ("date", "total_vaccinations", "people_fully_vaccinated")
    values = (parse_date(data), parse_total_vaccinations(data), parse_people_fully_vaccinated(data))
    data = dict(zip(keys, values))
    return pd.Series(data=data)


def parse_date(data: dict) -> str:
    date = data["Data"]
    date = str((pd.to_datetime(date, unit="ms").date() - pd.DateOffset(days=1)).date())
    return date


def parse_total_vaccinations(data: dict) -> int:
    return int(data["SZCZEPIENIA_SUMA"])


def parse_people_fully_vaccinated(data: dict) -> int:
    return int(data["DAWKA_2_SUMA"])


def add_totals(input: pd.Series) -> pd.Series:
    people_vaccinated = input['total_vaccinations'] - input['people_fully_vaccinated']
    return vaxutils.enrich_data(input, 'people_vaccinated', people_vaccinated)


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "Poland")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine', "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'source_url', "https://www.gov.pl/web/szczepimysie/raport-szczepien-przeciwko-covid-19")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(add_totals)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://services-eu1.arcgis.com/zk7YlClTgerl62BY/arcgis/rest/services/global_szczepienia_widok2/FeatureServer/0/query?f=json&where=Data%20BETWEEN%20(CURRENT_TIMESTAMP%20-%20INTERVAL%20%2724%27%20HOUR)%20AND%20CURRENT_TIMESTAMP&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&resultOffset=0&resultRecordCount=1&resultType=standard"
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
