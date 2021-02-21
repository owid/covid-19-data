import json
import datetime
import requests
import pandas as pd
import pytz
import vaxutils


def main():

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
        "https://dashboard.impactlebanon.com/s/public/elasticsearch/vaccine_appointment_data/_search?rest_total_hits_as_int=true&ignore_unavailable=true&ignore_throttled=true&preference=1613894483649&timeout=30000ms",
        json=js,
        headers=headers
    )
    request.raise_for_status()

    data = request.json()

    total_vaccinations = data["hits"]["total"]

    local_time = datetime.datetime.now(pytz.timezone("Asia/Beirut"))
    if local_time.hour < 8:
        local_time = local_time - datetime.timedelta(days=1)
    date = str(local_time.date())

    vaxutils.increment(
        location="Lebanon",
        total_vaccinations=total_vaccinations,
        date=date,
        source_url="https://impact.cib.gov.lb/home/dashboard/vaccine",
        vaccine="Pfizer/BioNTech",
    )


if __name__ == '__main__':
    main()
