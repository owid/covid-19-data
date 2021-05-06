import json
from datetime import datetime, timedelta

import requests
import pandas as pd

from vax.utils.incremental import enrich_data, increment
from vax.utils.files import load_query


class Poland:

    def __init__(self, source_url: str, source_url_ref: str, location: str, columns_rename: dict = None):
        self.source_url = source_url
        self.source_url_ref = source_url_ref
        self.location = location
        self.columns_rename = columns_rename

    @property
    def output_file(self):
        return f"./output/{self.location}.csv"

    def read(self) -> pd.Series:
        params = load_query('poland-all', to_str=False)
        data = requests.get(self.source_url, params=params).json()["features"][0]["attributes"]
        return pd.Series(data)

    def pipe_rename_columns(self, ds: pd.Series) -> pd.Series:
        return ds.rename(self.columns_rename)
    
    def pipe_date(self, ds: pd.Series) -> pd.Series:
        ds.loc["date"] = (
            (datetime.fromtimestamp(ds.date/1000) - timedelta(days=1))
            .strftime("%Y-%m-%d")
        )
        return ds

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, 'location', self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, 'vaccine', "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, 'source_url', self.source_url_ref)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return (
            ds
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_date)
            .pipe(self.pipe_location)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_source)
        )

    def to_csv(self, output_file: str = None):
        """Generalized."""
        data = self.read().pipe(self.pipeline)
        increment(
            location=data['location'],
            total_vaccinations=data['total_vaccinations'],
            people_vaccinated=data['people_vaccinated'],
            people_fully_vaccinated=data['people_fully_vaccinated'],
            date=data['date'],
            source_url=data['source_url'],
            vaccine=data['vaccine']
        )


def main():
    Poland(
        source_url=(
            "https://services-eu1.arcgis.com/zk7YlClTgerl62BY/ArcGIS/rest/services/global_szczepienia_actual_widok3/"
            "FeatureServer/0/query"
        ),
        source_url_ref="https://www.gov.pl/web/szczepimysie/raport-szczepien-przeciwko-covid-19",
        location="Poland",
        columns_rename={
            "SZCZEPIENIA_SUMA": "total_vaccinations",
            "DAWKA_1_SUMA": "people_vaccinated",
            "zaszczepieni_finalnie": "people_fully_vaccinated",
            "Data": "date"
        }
    ).to_csv() 


if __name__ == "__main__":
    main()
