import json
from datetime import datetime

import requests
import pandas as pd

from vax.utils.incremental import enrich_data, increment
from vax.utils.files import load_query

firstDose_source = (
    "https://services-eu1.arcgis.com/z6bHNio59iTqqSUY/arcgis/rest/services/Covid19_Vaccine_Administration_Hosted_View/"
    "FeatureServer/0/query?f=json&where=1%3D1&outFields=*&returnGeometry=false&"
    "outStatistics=%5B%7B%22onStatisticField%22%3A%22firstDose%22%2C%22outStatisticFieldName%22%3A%22firstDose_max%22%"
    "2C%22statisticType%22%3A%22max%22%7D%5D"
)
secondDose_source = (
    "https://services-eu1.arcgis.com/z6bHNio59iTqqSUY/arcgis/rest/services/Covid19_Vaccine_Administration_Hosted_View/"
    "FeatureServer/0/query?f=json&where=1%3D1&outFields=*&returnGeometry=false&"
    "outStatistics=%5B%7B%22onStatisticField%22%3A%22secondDose%22%2C%22outStatisticFieldName%22%3A%22secondDose_max"
    "%22%2C%22statisticType%22%3A%22max%22%7D%5D"
)
date_source = (
    "https://services-eu1.arcgis.com/z6bHNio59iTqqSUY/arcgis/rest/services/Covid19_Vaccine_Administration_Hosted_View/"
    "FeatureServer/0/query?f=json&where=1%3D1&outFields=*&returnGeometry=false&"
    "outStatistics=%5B%7B%22onStatisticField%22%3A%22relDate%22%2C%22outStatisticFieldName%22%3A%22relDate_max%22%2C"
    "%22statisticType%22%3A%22max%22%7D%5D"
)


class Ireland:

    def __init__(self, source_url: str, location: str, columns_rename: dict = None):
        self.source_url = source_url
        self.location = location
        self.columns_rename = columns_rename
        self.endpoint_doses = (
            "https://services-eu1.arcgis.com/z6bHNio59iTqqSUY/arcgis/rest/services/"
            "Covid19_Vaccine_Administration_Hosted_View/FeatureServer/0/query"
        )
        self.endpoint_vaccines_manufacturer = (
            "https://services-eu1.arcgis.com/z6bHNio59iTqqSUY/arcgis/rest/services/"
            "Covid19_Vaccine_Administration_VaccineTypeHostedView_V2/"
            "FeatureServer/0/query"
        )

    @property
    def output_file(self):
        return f"./output/{self.location}.csv"

    def read(self) -> pd.Series:
        ds = pd.Series({
            **self.parse_doses(),
            **self.parse_vaccines_manufacturer(),
        })
        if ds.date != ds.date_:
            raise ValueError(
                f"The API Endpoints are not synchronized, they report data for different dates: {ds.date}"
                f" and {ds.date_}"
            )
        return ds

    def parse_doses(self) -> str:
        params = load_query('ireland-doses', to_str=False)
        data = requests.get(self.endpoint_doses, params=params).json() 
        res = data["features"][0]["attributes"]
        return {
            "dose_1": res['firstDose'],
            "dose_2": res['secondDose'],
            "date": res['relDate']
        }

    def parse_vaccines_manufacturer(self):
        params = load_query('ireland-doses-manufacturer', to_str=False)
        data = requests.get(self.endpoint_vaccines_manufacturer, params=params).json() 
        res = data["features"][0]["attributes"]
        return {
            "pfizer": res['pf'],
            "oxford": res['az'],
            "moderna": res['modern'],
            "johnson": res['janssen'],
            "date_": res['relDate']
        }

    def pipe_people_vaccinated(self, ds: pd.Series) -> pd.Series:
        return ds.rename({
            "dose_1": "people_vaccinated"
        })

    def pipe_people_fully_vaccinated(self, ds: pd.Series) -> pd.Series:
        return ds.rename({
            "dose_2": "people_fully_vaccinated"
        })

    def pipe_total_vaccinations(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds, 
            "total_vaccinations", 
            ds.pfizer + ds.oxford + ds.moderna + ds.johnson
        )

    def pipe_date(self, ds: pd.Series) -> pd.Series:
        date_str = datetime.fromtimestamp(ds.date/1000).strftime("%Y-%m-%d")
        ds.loc["date"] = date_str
        return ds

    def pipe_corrections_1_dose(self, ds: pd.Series) -> pd.Series:
        ds.loc["people_vaccinated"] = ds.people_vaccinated + ds.johnson
        return ds

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, 'location', "Ireland")

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        vaccines = []
        if ds.pfizer > 0:
            vaccines.append("Pfizer/BioNTech")
        if ds.oxford > 0:
            vaccines.append("Oxford/AstraZeneca")
        if ds.moderna > 0:
            vaccines.append("Moderna")
        if ds.johnson > 0:
            vaccines.append("Johnson&Johnson")
        vaccines = ', '.join(sorted(vaccines))
        return enrich_data(ds, 'vaccine', vaccines)


    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, 'source_url', "https://covid19ireland-geohive.hub.arcgis.com/")

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return (
            ds
            .pipe(self.pipe_people_vaccinated)
            .pipe(self.pipe_people_fully_vaccinated)
            .pipe(self.pipe_total_vaccinations)
            # .pipe(self.pipe_corrections_1_dose)
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
    Ireland(
        source_url="https://covid19ireland-geohive.hub.arcgis.com/",
        location="Ireland"
    ).to_csv() 


if __name__ == "__main__":
    main()
