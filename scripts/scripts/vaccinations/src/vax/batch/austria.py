import os
import re

import pandas as pd


vaccine_mapping = {
    "BioNTechPfizer": "Pfizer/BioNTech",
    "Moderna": "Moderna",
    "AstraZeneca": "Oxford/AstraZeneca",
    "Janssen": "Johnson&Johnson",
}


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source, sep=";")


def filter_country(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["Name"] == "Österreich"]


def select_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    return df[columns]


def rename_columns(df: pd.DataFrame, columns: dict) -> pd.DataFrame:
    return df.rename(columns=columns)


def format_date(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(date=df.date.str.slice(0, 10))


def filter_rows(df: pd.DataFrame) -> pd.DataFrame:
    return df[df.total_vaccinations >= df.people_vaccinated]


def _get_vaccine_names(df: pd.DataFrame, translate: bool = False):
    ignore_fields = ['', 'Pro']
    regex_vaccines = r'EingetrageneImpfungen([a-zA-Z]*).*'
    vaccine_names = sorted(set(
        re.search(regex_vaccines, col).group(1) for col in df.columns if re.match(regex_vaccines, col)
    ))
    vaccine_names = [vax for vax in vaccine_names if vax not in ignore_fields]
    if translate:
        return sorted([vaccine_mapping[v] for v in vaccine_names])
    else:
        return sorted(vaccine_names)


def _check_vaccine_names(df: pd.DataFrame) -> pd.DataFrame:
    vaccine_names = _get_vaccine_names(df)
    unknown_vaccines = set(vaccine_names).difference(vaccine_mapping.keys())
    if unknown_vaccines:
        raise ValueError("Found unknown vaccines: {}".format(unknown_vaccines))
    return df


def enrich_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(
        location="Austria",
        source_url="https://info.gesundheitsministerium.gv.at/opendata/",
    )
    df = df.assign(vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")
    df.loc[df.date > '2021-03-23', "vaccine"] = "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
    return df


def pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .pipe(filter_country)
        .pipe(_check_vaccine_names)
        .pipe(select_columns, columns=["Datum", "Teilgeimpfte", "Vollimmunisierte", "EingetrageneImpfungen"])
        .pipe(rename_columns, columns={
            "Datum": "date",
            "Teilgeimpfte": "people_vaccinated",
            "Vollimmunisierte": "people_fully_vaccinated",
            "EingetrageneImpfungen": "total_vaccinations"
        })
        .pipe(filter_rows)
        .pipe(format_date)
        .pipe(enrich_columns)
        .sort_values("date")
    )


def main(paths):
    source = "https://info.gesundheitsministerium.gv.at/data/timeline-eimpfpass.csv"
    destination = paths.tmp_vax_loc("Austria")
    read(source).pipe(pipeline).to_csv(destination, index=False)
