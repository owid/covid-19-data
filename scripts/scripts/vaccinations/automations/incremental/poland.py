import json
import requests
import vaxutils
import pandas as pd


def read(source: str) -> pd.Series:
    data = json.loads(requests.get(source).content)["features"][0]["attributes"]
    return parse_data(data)


def parse_data(data: pd.DataFrame) -> pd.Series:
    data = {'date': parse_date(data), 'total_vaccinations': parse_total_vaccinations(data),
            'people_fully_vaccinated': parse_people_fully_vaccinated(data)}
    return pd.Series(data=data)


def parse_date(data: pd.DataFrame) -> str:
    date = data["Data"]
    date = str((pd.to_datetime(date, unit="ms").date() - pd.DateOffset(days=1)).date())
    return date


def parse_total_vaccinations(data: pd.DataFrame) -> str:
    total_vaccinations = data["SZCZEPIENIA_SUMA"]
    return total_vaccinations


def parse_people_fully_vaccinated(data: pd.DataFrame) -> str:
    people_fully_vaccinated = data["DAWKA_2_SUMA"]
    return people_fully_vaccinated


def add_totals(input: pd.Series) -> pd.Series:
    people_vaccinated = int(input['total_vaccinations']) - int(input['people_fully_vaccinated'])
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
    source = "https://services-eu1.arcgis.com/zk7YlClTgerl62BY/arcgis/rest/services/global_szczepienia_widok3/FeatureServer/0/query?f=json&where=Data%20BETWEEN%20(CURRENT_TIMESTAMP%20-%20INTERVAL%20%2724%27%20HOUR)%20AND%20CURRENT_TIMESTAMP&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&resultOffset=0&resultRecordCount=1&resultType=standard"
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
