import datetime

import pandas as pd
import pytz

from vax.utils.incremental import enrich_data, increment


def read(source: str) -> pd.Series:
    return pd.read_json(source).pipe(lambda ds: pd.DataFrame.from_records(ds["stats"]).iloc[0])


def translate_index(ds: pd.Series) -> pd.Series:
    return ds.rename({
        'first_vaccine_number': 'people_vaccinated',
        'second_vaccine_number': 'people_fully_vaccinated',
    })


def add_totals(ds: pd.Series) -> pd.Series:
    total_vaccinations = int(ds['people_vaccinated']) + int(ds['people_fully_vaccinated'])
    return enrich_data(ds, 'total_vaccinations', total_vaccinations)


def format_date(ds: pd.Series) -> pd.Series:
    date = str(datetime.datetime.now(pytz.timezone("Atlantic/Faeroe")).date() - datetime.timedelta(days=1))
    return enrich_data(ds, 'date', date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'location', "Faeroe Islands")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'vaccine', "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'source_url', "https://corona.fo/api")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(translate_index)
        .pipe(add_totals)
        .pipe(format_date)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main():
    source = "https://corona.fo/json/stats"
    data = read(source).pipe(pipeline)
    increment(
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
