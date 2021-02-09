import datetime
import json
import requests
import pytz
import vaxutils


def get_data():
    url = 'https://wabi-west-europe-b-primary-api.analysis.windows.net/public/reports/querydata?synchronous=true'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:85.0) Gecko/20100101 Firefox/85.0',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3',
        'X-PowerBI-ResourceKey': 'a01ccc19-80ba-4458-9849-66d7e1e300b7',
        'Content-Type': 'application/json;charset=UTF-8',
        'Origin': 'https://app.powerbi.com',
        'Referer': 'https://app.powerbi.com/view?r=eyJrIjoiYTAxY2NjMTktODBiYS00NDU4LTk4NDktNjZkN2UxZTMwMGI3IiwidCI6IjM5YzAwODM2LWVkMTItNDhkYS05Yjk3LTU5NGQ4MDhmMDNlNSIsImMiOjl9',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
    }
    data = '{"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"m","Entity":"Medway","Type":0}],"Select":[{"Column":{"Expression":{"SourceRef":{"Source":"m"}},"Property":"Dose schedule"},"Name":"Medway.Dose schedule"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"m"}},"Property":"Date of vaccination"}},"Function":5},"Name":"CountNonNull(Medway.Date of vaccination)"}],"Where":[{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"m"}},"Property":"Dose schedule"}}],"Values":[[{"Literal":{"Value":"\'First dose\'"}}],[{"Literal":{"Value":"\'Second dose\'"}}]]}}},{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"m"}},"Property":"Does the patient consent for a course of COVID-19 vaccination?"}}],"Values":[[{"Literal":{"Value":"\'Yes - Patient does consent\'"}}],[{"Literal":{"Value":"\'Yes - Consent is recorded\'"}}]]}}}],"OrderBy":[{"Direction":2,"Expression":{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"m"}},"Property":"Date of vaccination"}},"Function":5}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0,1]}]},"DataReduction":{"DataVolume":4,"Primary":{"Window":{"Count":1000}}},"Version":1}}}]},"CacheKey":"{\\"Commands\\":[{\\"SemanticQueryDataShapeCommand\\":{\\"Query\\":{\\"Version\\":2,\\"From\\":[{\\"Name\\":\\"m\\",\\"Entity\\":\\"Medway\\",\\"Type\\":0}],\\"Select\\":[{\\"Column\\":{\\"Expression\\":{\\"SourceRef\\":{\\"Source\\":\\"m\\"}},\\"Property\\":\\"Dose schedule\\"},\\"Name\\":\\"Medway.Dose schedule\\"},{\\"Aggregation\\":{\\"Expression\\":{\\"Column\\":{\\"Expression\\":{\\"SourceRef\\":{\\"Source\\":\\"m\\"}},\\"Property\\":\\"Date of vaccination\\"}},\\"Function\\":5},\\"Name\\":\\"CountNonNull(Medway.Date of vaccination)\\"}],\\"Where\\":[{\\"Condition\\":{\\"In\\":{\\"Expressions\\":[{\\"Column\\":{\\"Expression\\":{\\"SourceRef\\":{\\"Source\\":\\"m\\"}},\\"Property\\":\\"Dose schedule\\"}}],\\"Values\\":[[{\\"Literal\\":{\\"Value\\":\\"\'First dose\'\\"}}],[{\\"Literal\\":{\\"Value\\":\\"\'Second dose\'\\"}}]]}}},{\\"Condition\\":{\\"In\\":{\\"Expressions\\":[{\\"Column\\":{\\"Expression\\":{\\"SourceRef\\":{\\"Source\\":\\"m\\"}},\\"Property\\":\\"Does the patient consent for a course of COVID-19 vaccination?\\"}}],\\"Values\\":[[{\\"Literal\\":{\\"Value\\":\\"\'Yes - Patient does consent\'\\"}}],[{\\"Literal\\":{\\"Value\\":\\"\'Yes - Consent is recorded\'\\"}}]]}}}],\\"OrderBy\\":[{\\"Direction\\":2,\\"Expression\\":{\\"Aggregation\\":{\\"Expression\\":{\\"Column\\":{\\"Expression\\":{\\"SourceRef\\":{\\"Source\\":\\"m\\"}},\\"Property\\":\\"Date of vaccination\\"}},\\"Function\\":5}}}]},\\"Binding\\":{\\"Primary\\":{\\"Groupings\\":[{\\"Projections\\":[0,1]}]},\\"DataReduction\\":{\\"DataVolume\\":4,\\"Primary\\":{\\"Window\\":{\\"Count\\":1000}}},\\"Version\\":1}}}]}","QueryId":"","ApplicationContext":{"DatasetId":"819a1554-706f-4e7e-9f7d-ec4bf4a353e2","Sources":[{"ReportId":"a1d3f3f4-2b99-4dda-82af-e751394400c5"}]}}],"cancelQueries":[],"modelId":1616759}'
    return json.loads(requests.post(url, headers=headers, data=data).content)


def main():

    data = get_data()

    people_vaccinated = data["results"][0]["result"]["data"]["dsr"]["DS"][0]["PH"][0]["DM0"][0]["C"][1]
    people_fully_vaccinated = data["results"][0]["result"]["data"]["dsr"]["DS"][0]["PH"][0]["DM0"][1]["C"][1]
    total_vaccinations = people_vaccinated + people_fully_vaccinated

    date = str(datetime.datetime.now(pytz.timezone("Europe/Isle_of_Man")).date())

    vaxutils.increment(
        location="Isle of Man",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url="https://covid19.gov.im/general-information/covid-19-vaccination-statistics/",
        vaccine="Oxford/AstraZeneca, Pfizer/BioNTech"
    )


if __name__ == '__main__':
    main()