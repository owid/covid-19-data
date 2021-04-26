import datetime
import pytz
import requests

import pandas as pd

from vax.utils.incremental import enrich_data, increment


def run_query(url: str, query: str) -> int:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:88.0) Gecko/20100101 Firefox/88.0',
        'Accept': 'application/json, text/plain, */*',
        'X-PowerBI-ResourceKey': 'ea1f0f37-d994-4fa0-8871-78602545d370',
        'Content-Type': 'application/json;charset=UTF-8',
        'Origin': 'https://app.powerbi.com',
        'Connection': 'keep-alive',
        'Referer': 'https://app.powerbi.com/',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
    }
    result = requests.post(url, headers=headers, data=query).json()
    return result


def read(url: str) -> pd.Series:
    dose2_query = '''
    {"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"p","Entity":"P Regional_Vaccinations","Type":0}],"Select":[{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"p"}},"Property":"Sum of 2nd Dose"}},"Function":0},"Name":"Sum(P Regional_Vaccinations.Sum of 2nd Dose)"}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0]}]},"DataReduction":{"DataVolume":3,"Primary":{"Top":{}}},"Version":1},"ExecutionMetricsKind":1}}]},"CacheKey":"{\"Commands\":[{\"SemanticQueryDataShapeCommand\":{\"Query\":{\"Version\":2,\"From\":[{\"Name\":\"p\",\"Entity\":\"P Regional_Vaccinations\",\"Type\":0}],\"Select\":[{\"Aggregation\":{\"Expression\":{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"p\"}},\"Property\":\"Sum of 2nd Dose\"}},\"Function\":0},\"Name\":\"Sum(P Regional_Vaccinations.Sum of 2nd Dose)\"}]},\"Binding\":{\"Primary\":{\"Groupings\":[{\"Projections\":[0]}]},\"DataReduction\":{\"DataVolume\":3,\"Primary\":{\"Top\":{}}},\"Version\":1},\"ExecutionMetricsKind\":1}}]}","QueryId":"","ApplicationContext":{"DatasetId":"4d37c8f9-c7c5-4c69-9b89-cca38ce4ed7b","Sources":[{"ReportId":"bf70ff3f-0214-41fc-9e12-7f99700f4e00","VisualId":"d363f66070e7223e0520"}]}}],"cancelQueries":[],"modelId":4598049}
    '''
    return pd.Series(data={
        "people_fully_vaccinated": run_query(url, dose2_query)
    })


def enrich_date(input: pd.Series) -> pd.Series:
    date = str(datetime.datetime.now(pytz.timezone("Asia/Manila")).date() - datetime.timedelta(days=1))
    return enrich_data(input, 'date', date)


def enrich_location(input: pd.Series) -> pd.Series:
    return enrich_data(input, "location", "Philippines")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return enrich_data(input, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(input: pd.Series) -> pd.Series:
    return enrich_data(input, "source_url", "https://www.covid19.gov.ph/")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input
        .pipe(enrich_date)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main():

    url = "https://wabi-south-east-asia-api.analysis.windows.net/public/reports/querydata?synchronous=true"
    data = read(url).pipe(pipeline)
    increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()
