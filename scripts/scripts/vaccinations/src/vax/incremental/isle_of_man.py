import datetime
import json
import pytz
import requests
import pandas as pd
from vax.utils.incremental import enrich_data, increment


def read(source: str) -> pd.Series:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:85.0) Gecko/20100101 Firefox/85.0',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3',
        'X-PowerBI-ResourceKey': 'a01ccc19-80ba-4458-9849-66d7e1e300b7',
        'Content-Type': 'application/json;charset=UTF-8',
        'Origin': 'https://app.powerbi.com',
        'Referer': (
            "https://app.powerbi.com/view?r=eyJrIjoiYTAxY2NjMTktODBiYS00NDU4LTk4NDktNjZkN2UxZTMwMGI3IiwidCI6IjM"
            "5YzAwODM2LWVkMTItNDhkYS05Yjk3LTU5NGQ4MDhmMDNlNSIsImMiOjl9"
        ),
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
    }
    data = '{"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"m","Entity":"Medway","Type":0}],"Select":[{"Column":{"Expression":{"SourceRef":{"Source":"m"}},"Property":"Dose schedule"},"Name":"Medway.Dose schedule"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"m"}},"Property":"Date of vaccination"}},"Function":5},"Name":"CountNonNull(Medway.Date of vaccination)"}],"Where":[{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"m"}},"Property":"Dose schedule"}}],"Values":[[{"Literal":{"Value":"\'First dose\'"}}],[{"Literal":{"Value":"\'Second dose\'"}}]]}}}],"OrderBy":[{"Direction":2,"Expression":{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"m"}},"Property":"Date of vaccination"}},"Function":5}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0,1]}]},"DataReduction":{"DataVolume":4,"Primary":{"Window":{"Count":1000}}},"Version":1}}}]},"CacheKey":"{\\"Commands\\":[{\\"SemanticQueryDataShapeCommand\\":{\\"Query\\":{\\"Version\\":2,\\"From\\":[{\\"Name\\":\\"m\\",\\"Entity\\":\\"Medway\\",\\"Type\\":0}],\\"Select\\":[{\\"Column\\":{\\"Expression\\":{\\"SourceRef\\":{\\"Source\\":\\"m\\"}},\\"Property\\":\\"Dose schedule\\"},\\"Name\\":\\"Medway.Dose schedule\\"},{\\"Aggregation\\":{\\"Expression\\":{\\"Column\\":{\\"Expression\\":{\\"SourceRef\\":{\\"Source\\":\\"m\\"}},\\"Property\\":\\"Date of vaccination\\"}},\\"Function\\":5},\\"Name\\":\\"CountNonNull(Medway.Date of vaccination)\\"}],\\"Where\\":[{\\"Condition\\":{\\"In\\":{\\"Expressions\\":[{\\"Column\\":{\\"Expression\\":{\\"SourceRef\\":{\\"Source\\":\\"m\\"}},\\"Property\\":\\"Dose schedule\\"}}],\\"Values\\":[[{\\"Literal\\":{\\"Value\\":\\"\'First dose\'\\"}}],[{\\"Literal\\":{\\"Value\\":\\"\'Second dose\'\\"}}]]}}}],\\"OrderBy\\":[{\\"Direction\\":2,\\"Expression\\":{\\"Aggregation\\":{\\"Expression\\":{\\"Column\\":{\\"Expression\\":{\\"SourceRef\\":{\\"Source\\":\\"m\\"}},\\"Property\\":\\"Date of vaccination\\"}},\\"Function\\":5}}}]},\\"Binding\\":{\\"Primary\\":{\\"Groupings\\":[{\\"Projections\\":[0,1]}]},\\"DataReduction\\":{\\"DataVolume\\":4,\\"Primary\\":{\\"Window\\":{\\"Count\\":1000}}},\\"Version\\":1}}}]}","QueryId":"","ApplicationContext":{"DatasetId":"819a1554-706f-4e7e-9f7d-ec4bf4a353e2","Sources":[{"ReportId":"a1d3f3f4-2b99-4dda-82af-e751394400c5"}]}}],"cancelQueries":[],"modelId":1616759}'  # noqa: E501

    df = (
        json.loads(
            requests.post(source, headers=headers, data=data).content
        )["results"][0]["result"]["data"]["dsr"]["DS"][0]["PH"][0]["DM0"]
    )
    return parse_data(df)


def parse_data(df: pd.DataFrame) -> pd.Series:
    keys = ("people_vaccinated", "people_fully_vaccinated")
    values = (parse_people_vaccinated(df), parse_people_fully_vaccinated(df))
    data = dict(zip(keys, values))
    return pd.Series(data=data)


def parse_people_fully_vaccinated(df: dict) -> int:
    people_fully_vaccinated = [elem["C"][1] for elem in df if elem["C"][0] == "Second dose"][0]
    return people_fully_vaccinated


def parse_people_vaccinated(df: dict) -> int:
    people_vaccinated = [elem["C"][1] for elem in df if elem["C"][0] == "First dose"][0]
    return people_vaccinated


def add_totals(ds: pd.Series) -> pd.Series:
    total_vaccinations = ds['people_vaccinated'] + ds['people_fully_vaccinated']
    return enrich_data(ds, 'total_vaccinations', total_vaccinations)


def format_date(ds: pd.Series) -> pd.Series:
    date = str(datetime.datetime.now(pytz.timezone("Europe/Isle_of_Man")).date())
    return enrich_data(ds, 'date', date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'location', "Isle of Man")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'vaccine', "Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'source_url', "https://covid19.gov.im/general-information/covid-19-vaccination-statistics/")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(add_totals)
        .pipe(format_date)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main():
    source = 'https://wabi-west-europe-b-primary-api.analysis.windows.net/public/reports/querydata?synchronous=true'
    data = read(source).pipe(pipeline)
    increment(
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
