import datetime
import pytz
import requests
import pandas as pd
import vaxutils
import json


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
    js = json.loads('''
        {"aggs":{},"size":0,"stored_fields":["*"],"script_fields":{},"docvalue_fields":[{"field":"@timestamp","format":"date_time"},{"field":"event_creation_date","format":"date_time"},{"field":"event_start_date_time","format":"date_time"},{"field":"event_stop_date_time","format":"date_time"},{"field":"patient_create_date","format":"date_time"},{"field":"patient_date_of_birth","format":"date_time"}],"_source":{"excludes":[]},"query":{"bool":{"must":[],"filter":[{"match_all":{}},{"bool":{"must_not":{"bool":{"should":[{"match":{"vaccine_registration_is_duplicate":1}}],"minimum_should_match":1}}}},{"match_phrase":{"event_stage.keyword":"2.DONE"}},{"range":{"patient_create_date":{"gte":"2021-01-22T08:01:29.561Z","lte":"2021-02-21T08:01:29.561Z","format":"strict_date_optional_time"}}}],"should":[],"must_not":[]}}}
    ''')

    request = requests.post(
        source,
        json=js,
        headers=headers
    )
    request.raise_for_status()
    df = request.json()
    return parse_data(df)


def parse_data(df: pd.DataFrame) -> pd.Series:
    return pd.Series({'total_vaccinations': parse_total_vaccinations(df)})


def parse_total_vaccinations(df: pd.DataFrame) -> int:
    return int(df["hits"]["total"])


def format_date(ds: pd.Series) -> pd.Series:
    local_time = datetime.datetime.now(pytz.timezone("Asia/Beirut"))
    if local_time.hour < 8:
        local_time = local_time - datetime.timedelta(days=1)
    date = str(local_time.date())
    return vaxutils.enrich_data(ds, 'date', date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'location', "Lebanon")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(ds, 'vaccine', "Pfizer/BioNTech")


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
        date=data['date'],
        source_url=data['source_url'],
        vaccine=data['vaccine']
    )


if __name__ == "__main__":
    main()
