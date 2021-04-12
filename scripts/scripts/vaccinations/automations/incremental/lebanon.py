import datetime
import pytz
import requests
import pandas as pd
import vaxutils
import json


def get_api_value(source: str, query: str, headers: dict):
    query = json.loads(query)
    data = requests.post(source, json=query, headers=headers).json()
    value = int(data["hits"]["total"])
    return value


def read(source: str) -> pd.Series:

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.16; rv:85.0) Gecko/20100101 Firefox/85.0",
        "Accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "kbn-version": "7.6.1",
        "Origin": "https://dashboard.impactlebanon.com",
        "Connection": "keep-alive",
        "Referer": "https://dashboard.impactlebanon.com/s/public/app/kibana",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }

    total_vaccinations_query = '''
        {"aggs":{"2":{"filters":{"filters":{"Healthcare workers":{"bool":{"must":[],"filter":[{"bool":{"should":[{"match":{"patient_health_care_worker":true}}],"minimum_should_match":1}}],"should":[],"must_not":[]}},"Other categories":{"bool":{"must":[],"filter":[{"bool":{"should":[{"match":{"patient_health_care_worker":false}}],"minimum_should_match":1}}],"should":[],"must_not":[]}}}}}},"size":0,"stored_fields":["*"],"script_fields":{},"docvalue_fields":[{"field":"@timestamp","format":"date_time"},{"field":"event_creation_date","format":"date_time"},{"field":"event_post_date","format":"date_time"},{"field":"event_start_date_time","format":"date_time"},{"field":"event_stop_date_time","format":"date_time"},{"field":"patient_create_date","format":"date_time"},{"field":"patient_date_of_birth","format":"date_time"}],"_source":{"excludes":[]},"query":{"bool":{"must":[],"filter":[{"match_all":{}},{"bool":{"must_not":{"bool":{"should":[{"match":{"vaccine_registration_is_duplicate":1}}],"minimum_should_match":1}}}},{"match_phrase":{"event_stage.keyword":"2.DONE"}},{"range":{"event_creation_date":{"gte":"2018-06-01T04:51:47.181Z","lte":"2023-05-01T04:51:23.196Z","format":"strict_date_optional_time"}}}],"should":[],"must_not":[]}}}
    '''
    total_vaccinations = get_api_value(source, total_vaccinations_query, headers)

    people_fully_vaccinated_query = '''
        {"aggs":{},"size":0,"stored_fields":["*"],"script_fields":{},"docvalue_fields":[{"field":"@timestamp","format":"date_time"},{"field":"event_creation_date","format":"date_time"},{"field":"event_post_date","format":"date_time"},{"field":"event_start_date_time","format":"date_time"},{"field":"event_stop_date_time","format":"date_time"},{"field":"patient_create_date","format":"date_time"},{"field":"patient_date_of_birth","format":"date_time"}],"_source":{"excludes":[]},"query":{"bool":{"must":[],"filter":[{"bool":{"should":[{"query_string":{"fields":["event_name"],"query":"*Dose 2*"}}],"minimum_should_match":1}},{"bool":{"must_not":{"bool":{"should":[{"match":{"vaccine_registration_is_duplicate":1}}],"minimum_should_match":1}}}},{"match_phrase":{"event_stage.keyword":"2.DONE"}},{"range":{"event_creation_date":{"gte":"2018-06-01T04:51:47.181Z","lte":"2023-05-01T04:51:23.196Z","format":"strict_date_optional_time"}}}],"should":[],"must_not":[]}}}
    '''
    people_fully_vaccinated = get_api_value(source, people_fully_vaccinated_query, headers)

    people_vaccinated = total_vaccinations - people_fully_vaccinated
    
    return pd.Series({
        "total_vaccinations": total_vaccinations,
        "people_fully_vaccinated": people_fully_vaccinated,
        "people_vaccinated": people_vaccinated
    })


def format_date(ds: pd.Series) -> pd.Series:
    local_time = datetime.datetime.now(pytz.timezone("Asia/Beirut")) - datetime.timedelta(days=1)
    date = str(local_time.date())
    return vaxutils.enrich_data(ds, 'date', date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'location', "Lebanon")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'vaccine', "Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sputnik V")


def enrich_source(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'source_url', "https://impact.cib.gov.lb/home/dashboard/vaccine")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds.pipe(format_date)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://dashboard.impactlebanon.com/s/public/elasticsearch/vaccine_appointment_data/_search?rest_total_hits_as_int=true&ignore_unavailable=true&ignore_throttled=true&preference=1613894483649&timeout=30000ms"
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
