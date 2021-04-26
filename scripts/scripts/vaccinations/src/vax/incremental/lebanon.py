import datetime
import pytz
import requests
import pandas as pd
from vax.utils.incremental import enrich_data, increment
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
        {"aggs":{},"size":0,"stored_fields":["*"],"script_fields":{"vaccine_registration_age":{"script":{"source":"if (doc[\'vaccine_registration_date_of_birth\'].size()==0) {\\n    return -1 \\n}\\nelse {\\n    Instant instant = Instant.ofEpochMilli(new Date().getTime());\\nZonedDateTime birth = doc[\'vaccine_registration_date_of_birth\'].value;\\nZonedDateTime now = ZonedDateTime.ofInstant(instant, ZoneId.of(\'Z\'));\\nreturn ChronoUnit.YEARS.between(birth, now)\\n}\\n","lang":"painless"}}},"docvalue_fields":[{"field":"@timestamp","format":"date_time"},{"field":"batch_creation_date_time","format":"date_time"},{"field":"batch_proposed_vaccination_date","format":"date_time"},{"field":"event_last_updated_date_time","format":"date_time"},{"field":"last_updated_date_time","format":"date_time"},{"field":"vaccine_registration_covid_infection_date","format":"date_time"},{"field":"vaccine_registration_creation_date_time","format":"date_time"},{"field":"vaccine_registration_date","format":"date_time"},{"field":"vaccine_registration_date_of_birth","format":"date_time"},{"field":"vaccine_registration_event_creation_date_time","format":"date_time"},{"field":"vaccine_registration_event_date","format":"date_time"},{"field":"vaccine_registration_event_vaccination_date","format":"date_time"},{"field":"vaccine_registration_first_dose_vaccination_date","format":"date_time"},{"field":"vaccine_registration_previous_vaccine_date","format":"date_time"},{"field":"vaccine_registration_upload_date","format":"date_time"}],"_source":{"excludes":[]},"query":{"bool":{"must":[],"filter":[{"match_all":{}},{"bool":{"filter":[{"bool":{"must_not":{"bool":{"should":[{"match":{"vaccine_registration_is_duplicate":1}}],"minimum_should_match":1}}}},{"bool":{"filter":[{"bool":{"must_not":{"bool":{"should":[{"match":{"vaccine_registration_is_upload_vaccinated":1}}],"minimum_should_match":1}}}},{"bool":{"filter":[{"bool":{"should":[{"exists":{"field":"vaccine_registration_date_of_birth"}}],"minimum_should_match":1}},{"bool":{"filter":[{"bool":{"must_not":{"bool":{"should":[{"range":{"vaccine_registration_event_dose_number":{"gt":2}}}],"minimum_should_match":1}}}},{"bool":{"must_not":{"bool":{"should":[{"match_phrase":{"event_status.keyword":"CREATED"}}],"minimum_should_match":1}}}}]}}]}}]}}]}},{"match_phrase":{"event_status.keyword":"DONE"}},{"range":{"vaccine_registration_event_creation_date_time":{"gte":"2018-06-01T04:51:47.181Z","lte":"2023-05-01T04:51:23.196Z","format":"strict_date_optional_time"}}}],"should":[],"must_not":[]}}}
    '''
    total_vaccinations = get_api_value(source, total_vaccinations_query, headers)

    people_fully_vaccinated_query = '''
        {"aggs":{},"size":0,"stored_fields":["*"],"script_fields":{"vaccine_registration_age":{"script":{"source":"if (doc[\'vaccine_registration_date_of_birth\'].size()==0) {\\n    return -1 \\n}\\nelse {\\n    Instant instant = Instant.ofEpochMilli(new Date().getTime());\\nZonedDateTime birth = doc[\'vaccine_registration_date_of_birth\'].value;\\nZonedDateTime now = ZonedDateTime.ofInstant(instant, ZoneId.of(\'Z\'));\\nreturn ChronoUnit.YEARS.between(birth, now)\\n}\\n","lang":"painless"}}},"docvalue_fields":[{"field":"@timestamp","format":"date_time"},{"field":"batch_creation_date_time","format":"date_time"},{"field":"batch_proposed_vaccination_date","format":"date_time"},{"field":"event_last_updated_date_time","format":"date_time"},{"field":"last_updated_date_time","format":"date_time"},{"field":"vaccine_registration_covid_infection_date","format":"date_time"},{"field":"vaccine_registration_creation_date_time","format":"date_time"},{"field":"vaccine_registration_date","format":"date_time"},{"field":"vaccine_registration_date_of_birth","format":"date_time"},{"field":"vaccine_registration_event_creation_date_time","format":"date_time"},{"field":"vaccine_registration_event_date","format":"date_time"},{"field":"vaccine_registration_event_vaccination_date","format":"date_time"},{"field":"vaccine_registration_first_dose_vaccination_date","format":"date_time"},{"field":"vaccine_registration_previous_vaccine_date","format":"date_time"},{"field":"vaccine_registration_upload_date","format":"date_time"}],"_source":{"excludes":[]},"query":{"bool":{"must":[],"filter":[{"match_all":{}},{"bool":{"filter":[{"bool":{"must_not":{"bool":{"should":[{"match":{"vaccine_registration_is_duplicate":1}}],"minimum_should_match":1}}}},{"bool":{"filter":[{"bool":{"must_not":{"bool":{"should":[{"match":{"vaccine_registration_is_upload_vaccinated":1}}],"minimum_should_match":1}}}},{"bool":{"filter":[{"bool":{"should":[{"exists":{"field":"vaccine_registration_date_of_birth"}}],"minimum_should_match":1}},{"bool":{"filter":[{"bool":{"must_not":{"bool":{"should":[{"range":{"vaccine_registration_event_dose_number":{"gt":2}}}],"minimum_should_match":1}}}},{"bool":{"must_not":{"bool":{"should":[{"match_phrase":{"event_status.keyword":"CREATED"}}],"minimum_should_match":1}}}}]}}]}}]}}]}},{"match_phrase":{"event_status.keyword":"DONE"}},{"match_phrase":{"vaccine_registration_event_dose_number":"2"}},{"range":{"vaccine_registration_event_creation_date_time":{"gte":"2018-06-01T04:51:47.181Z","lte":"2023-05-01T04:51:23.196Z","format":"strict_date_optional_time"}}}],"should":[],"must_not":[]}}}
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
    return enrich_data(ds, 'date', date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'location', "Lebanon")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'vaccine', "Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sputnik V")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'source_url', "https://impact.cib.gov.lb/home/dashboard/vaccine")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds.pipe(format_date)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://dashboard.impactlebanon.com/s/public/elasticsearch/vaccine_registration_event_data/_search?rest_total_hits_as_int=true&ignore_unavailable=true&ignore_throttled=true&preference=1619368403040&timeout=30000ms"
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
