import os
import re
from typing import Dict

import pandas as pd


class Germany:

    def  __init__(self, source_url: str, source_url_ref: str, location: str, columns_rename: dict = None, 
                  vaccine_mapping: dict = None):
        self.source_url = source_url
        self.source_url_ref = source_url_ref
        self.location = location
        self.columns_rename = columns_rename
        self.vaccine_mapping = vaccine_mapping
        self.regex_doses_colnames = r"dosen_([a-zA-Z]*)_kumulativ"

    def read(self):
        return pd.read_csv(self.source_url, sep="\t")

    def _check_vaccines(self, df: pd.DataFrame):
        """Get vaccine columns mapped to Vaccine names."""
        EXCLUDE = ['kbv', 'dim']
        def _is_vaccine_column(column_name: str):
            if re.search(self.regex_doses_colnames, column_name):
                if re.search(self.regex_doses_colnames, column_name).group(1) not in EXCLUDE:
                    return True
            return False
        for column_name in df.columns:
            if _is_vaccine_column(column_name) and  column_name not in self.vaccine_mapping:
                    raise ValueError(f"Found unknown vaccine: {column_name}")
        return df

    def translate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.columns_rename)

    def translate_vaccine_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.vaccine_mapping)

    def enrich_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location="Germany")

    def pipeline_base(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self._check_vaccines)
            .pipe(self.translate_columns)
            .pipe(self.translate_vaccine_columns)
            .pipe(self.enrich_location)
        )

    def add_johnson_to_people_vaccinated(self, df: pd.DataFrame) -> pd.DataFrame:
        colname_johnson = "Johnson&Johnson"
        return df.assign(people_vaccinated=df.people_vaccinated + df[colname_johnson])

    def enrich_source(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(source_url=self.source_url_ref)

    def _vaccine_start_dates(self, df: pd.DataFrame):
        date2vax = sorted((
            (
                df.loc[df[vaccine] > 0, "date"].min(),
                vaccine
            )
            for vaccine in self.vaccine_mapping.values()
        ), key=lambda x: x[0], reverse=True)
        return [
            (
                date2vax[i][0],
                ", ".join(sorted(v[1] for v in date2vax[i:]))
            )
            for i in range(len(date2vax))
        ]

    def enrich_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        vax_date_mapping = self._vaccine_start_dates(df)
        def _enrich_vaccine(date: str) -> str:
            for dt, vaccines in vax_date_mapping:
                if date >= dt:
                    return vaccines
            raise ValueError(f"Invalid date {date} in DataFrame!")
        return df.assign(vaccine=df.date.apply(_enrich_vaccine))

    def select_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[[
            "date",
            "location",
            "vaccine",
            "source_url",
            "total_vaccinations",
            "people_vaccinated",
            "people_fully_vaccinated",
        ]]

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.add_johnson_to_people_vaccinated)
            .pipe(self.enrich_source)
            .pipe(self.enrich_vaccine)
            .pipe(self.select_output_columns)
        )

    def melt_manufacturers(self, df: pd.DataFrame) -> pd.DataFrame:
        id_vars = ["date", "location"]
        return df[id_vars + list(self.vaccine_mapping.values())].melt(
            id_vars=id_vars,
            var_name="vaccine",
            value_name="total_vaccinations"
        )
        
    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.melt_manufacturers)
        )

    def to_csv(self, paths):
        df_base = self.read().pipe(self.pipeline_base)
        # Export data
        df = df_base.pipe(self.pipeline)
        df.to_csv(
            paths.out_tmp(self.location),
            index=False
        )
        # Export manufacturer data
        df = df_base.pipe(self.pipeline_manufacturer)
        df.to_csv(
            paths.out_tmp_man(self.location),
            index=False
        )


def main(paths):
    Germany(
        source_url="https://impfdashboard.de/static/data/germany_vaccinations_timeseries_v2.tsv",
        source_url_ref="https://impfdashboard.de/",
        location="Germany",
        columns_rename={
            "dosen_kumulativ": "total_vaccinations",
            "personen_erst_kumulativ": "people_vaccinated",
            "personen_voll_kumulativ": "people_fully_vaccinated",
        },
        vaccine_mapping = {
            "dosen_biontech_kumulativ": "Pfizer/BioNTech",
            "dosen_moderna_kumulativ": "Moderna",
            "dosen_astrazeneca_kumulativ": "Oxford/AstraZeneca",
            "dosen_johnson_kumulativ": "Johnson&Johnson"
        }
    ).to_csv(paths)


if __name__ == "__main__":
    main()
