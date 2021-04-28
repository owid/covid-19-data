from datetime import datetime
import pytz
import requests

import pandas as pd

from vax.utils.incremental import enrich_data, increment
from vax.utils.files import load_query


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
    response = requests.post(url, headers=headers, data=query)
    if response.ok:
        data = response.json()
    else:
        raise "API response not valid. Recommendation: Check header"
    return data["results"][0]["result"]["data"]["dsr"]["DS"][0]["PH"][0]["DM0"][0]["M0"]


def read(url: str) -> pd.Series:
    date_str = parse_date(run_query(url, load_query("philippines-date")))
    return pd.Series(data={
        "total_vaccinations": run_query(url, load_query("philippines-total-vaccinations")),
        "people_vaccinated": run_query(url, load_query("philippines-people-vaccinated")),
        "people_fully_vaccinated": run_query(url, load_query("philippines-people-fully-vaccinated")),
        "date": date_str
    })


def parse_date(query_response: str):
    return datetime.strptime(query_response, 'as of %m/%d/%Y %I:%M %p').strftime("%Y-%m-%d")


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Philippines")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "source_url", "https://www.covid19.gov.ph/")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
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
