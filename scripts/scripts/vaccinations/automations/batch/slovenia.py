import requests

import pandas as pd


def read(source: str) -> pd.DataFrame:

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:87.0) Gecko/20100101 Firefox/87.0",
        "Accept": "application/json, text/plain, */*",
        "ActivityId": "a73e2035-2f0e-290b-319a-c10ebb699c77",
        "RequestId": "25da6f2b-7604-a99a-beef-8c3de4f59f67",
        "X-PowerBI-ResourceKey": "e868280f-1322-4be2-a19a-e9fc2112609f",
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": "https://app.powerbi.com",
        "Connection": "keep-alive",
        "Referer": "https://app.powerbi.com/",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }

    data = '{"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"c1","Entity":"Calendar","Type":0},{"Name":"c","Entity":"eRCO_podatki","Type":0}],"Select":[{"Column":{"Expression":{"SourceRef":{"Source":"c1"}},"Property":"Date"},"Name":"Calendar.Date"},{"Measure":{"Expression":{"SourceRef":{"Source":"c"}},"Property":"Weight running total in Date"},"Name":"eRCO_podatki.Weight running total in Date"},{"Column":{"Expression":{"SourceRef":{"Source":"c"}},"Property":"Odmerek"},"Name":"eRCO_podatki.Odmerek"}],"Where":[{"Condition":{"Comparison":{"ComparisonKind":1,"Left":{"Column":{"Expression":{"SourceRef":{"Source":"c1"}},"Property":"Date"}},"Right":{"DateSpan":{"Expression":{"Literal":{"Value":"datetime\'2020-12-26T01:00:00\'"}},"TimeUnit":5}}}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0,1]}]},"Secondary":{"Groupings":[{"Projections":[2]}]},"DataReduction":{"DataVolume":4,"Intersection":{"BinnedLineSample":{}}},"Version":1}}}]},"CacheKey":"{\\"Commands\\":[{\\"SemanticQueryDataShapeCommand\\":{\\"Query\\":{\\"Version\\":2,\\"From\\":[{\\"Name\\":\\"c1\\",\\"Entity\\":\\"Calendar\\",\\"Type\\":0},{\\"Name\\":\\"c\\",\\"Entity\\":\\"eRCO_podatki\\",\\"Type\\":0}],\\"Select\\":[{\\"Column\\":{\\"Expression\\":{\\"SourceRef\\":{\\"Source\\":\\"c1\\"}},\\"Property\\":\\"Date\\"},\\"Name\\":\\"Calendar.Date\\"},{\\"Measure\\":{\\"Expression\\":{\\"SourceRef\\":{\\"Source\\":\\"c\\"}},\\"Property\\":\\"Weight running total in Date\\"},\\"Name\\":\\"eRCO_podatki.Weight running total in Date\\"},{\\"Column\\":{\\"Expression\\":{\\"SourceRef\\":{\\"Source\\":\\"c\\"}},\\"Property\\":\\"Odmerek\\"},\\"Name\\":\\"eRCO_podatki.Odmerek\\"}],\\"Where\\":[{\\"Condition\\":{\\"Comparison\\":{\\"ComparisonKind\\":1,\\"Left\\":{\\"Column\\":{\\"Expression\\":{\\"SourceRef\\":{\\"Source\\":\\"c1\\"}},\\"Property\\":\\"Date\\"}},\\"Right\\":{\\"DateSpan\\":{\\"Expression\\":{\\"Literal\\":{\\"Value\\":\\"datetime\'2020-12-26T01:00:00\'\\"}},\\"TimeUnit\\":5}}}}}]},\\"Binding\\":{\\"Primary\\":{\\"Groupings\\":[{\\"Projections\\":[0,1]}]},\\"Secondary\\":{\\"Groupings\\":[{\\"Projections\\":[2]}]},\\"DataReduction\\":{\\"DataVolume\\":4,\\"Intersection\\":{\\"BinnedLineSample\\":{}}},\\"Version\\":1}}}]}","QueryId":"","ApplicationContext":{"DatasetId":"7b40529e-a50e-4dd3-8fe8-997894b4cdaa","Sources":[{"ReportId":"b201281d-b2e7-4470-9f4e-0b3063794c76"}]}}],"cancelQueries":[],"modelId":159824}'

    resp = requests.post(source, headers=headers, data=data).json()
    resp = resp["results"][0]["result"]["data"]["dsr"]["DS"][0]["PH"][0]["DM0"]

    parsed_data = []

    for element in resp:

        date = pd.to_datetime(element["G0"], unit="ms")
        people_vaccinated = element["X"][0]["M0"]
        people_fully_vaccinated = element["X"][1]["M0"] if len(element["X"]) > 1 else 0

        parsed_data.append({
            "date": date,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
        })

    df = pd.DataFrame.from_records(parsed_data)
    df["total_vaccinations"] = df.people_vaccinated + df.people_fully_vaccinated

    return df


def format_date(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        date=pd.to_datetime(input.date, unit="ms").astype(str)
    )


def enrich_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        location="Slovenia",
        source_url="https://www.cepimose.si/",
        vaccine="Oxford/AstraZeneca, Pfizer/BioNTech",
    )


def pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input
        .pipe(format_date)
        .pipe(enrich_columns)
        .sort_values("date")
    )


def main():
    source = "https://wabi-west-europe-e-primary-api.analysis.windows.net/public/reports/querydata?synchronous=true"
    destination = "automations/output/Slovenia.csv"
    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
