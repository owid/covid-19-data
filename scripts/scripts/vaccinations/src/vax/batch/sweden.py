import os
import datetime

import pandas as pd


class Sweden(object):

    def __init__(self, source_url: str, location: str, columns_rename: dict = None, columns_cumsum: list = None):
        """Constructor.

        Args:
            source_url (str): Source data url
            location (str): Location name
            columns_rename (dict, optional): Maps original to new names. Defaults to None.
            columns_cumsum (list, optional): List of columns to apply cumsum to. Comes handy when the values reported
                                                are daily. Defaults to None.
        """
        self.source_url = source_url
        self.location = location
        self.columns_rename = columns_rename

    def read(self) -> pd.DataFrame:
        daily = self._read_daily_data()
        weekly = self._read_weekly_data()
        weekly = weekly[weekly["date"] < daily["date"].min()]
        return pd.concat([daily, weekly]).sort_values("date").reset_index(drop=True)

    def enrich_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            location=self.location,
            source_url=self.source_url
        )

    def enrich_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(vaccine="Oxford/AstraZeneca, Pfizer/BioNTech")

    def exclude_data_points(self, df: pd.DataFrame) -> pd.DataFrame:
        # The data contains an error that creates a negative change in the people_vaccinated series
        df = df[df.date.astype(str) != "2021-04-11"]
        return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.enrich_vaccine)
            .pipe(self.enrich_columns)
            .pipe(self.exclude_data_points)
        )

    def _week_to_date(self, row: int):
        origin_date = pd.to_datetime("2019-12-29") if row.Vecka >= 52 else pd.to_datetime("2021-01-03")
        return origin_date + pd.DateOffset(days=7 * int(row.Vecka))

    def _read_weekly_data(self) -> pd.DataFrame:
        url = "https://fohm.maps.arcgis.com/sharing/rest/content/items/fc749115877443d29c2a49ea9eca77e9/data"
        df = pd.read_excel(url, sheet_name="Vaccinerade tidsserie")
        df = df[df["Region"] == "| Sverige |"][["Vecka", "Antal vaccinerade", "Vaccinationsstatus"]]
        df = df.pivot_table(values="Antal vaccinerade", index="Vecka", columns="Vaccinationsstatus").reset_index()
        # Week-to-date logic will stop working after 2021
        if not datetime.date.today().year < 2022:
            raise ValueError("Check the year! This script is not ready for 2022!")
        df.loc[:, "date"] = df.apply(self._week_to_date, axis=1).dt.date.astype(str)
        df = df.drop(columns=["Vecka"]).sort_values("date").rename(columns={
            "Minst 1 dos": "people_vaccinated", "Färdigvaccinerade": "people_fully_vaccinated"
        })
        df.loc[:, "total_vaccinations"] = df["people_vaccinated"] + df["people_fully_vaccinated"]
        return df

    def _read_daily_data(self) -> pd.DataFrame:
        url = (
            "https://www.folkhalsomyndigheten.se/smittskydd-beredskap/utbrott/aktuella-utbrott/covid-19/"
            "vaccination-mot-covid-19/statistik/statistik-over-registrerade-vaccinationer-covid-19/"
        )
        df = pd.read_html(url)[1]
        df = df[["Datum", "Antal vaccinerademed minst 1 dos*", "Antal vaccinerademed 2 doser"]].rename(columns={
            "Datum": "date",
            "Antal vaccinerademed minst 1 dos*": "people_vaccinated",
            "Antal vaccinerademed 2 doser": "people_fully_vaccinated",
        })
        df["people_vaccinated"] = df["people_vaccinated"].str.replace(r"\s", "", regex=True).astype(int)
        df["people_fully_vaccinated"] = df["people_fully_vaccinated"].str.replace(r"\s", "", regex=True).astype(int)
        df["total_vaccinations"] = df["people_vaccinated"] + df["people_fully_vaccinated"]
        return df

    def to_csv(self, paths):
        """Generalized."""
        df = self.read().pipe(self.pipeline)
        df.to_csv(paths.out_tmp(self.location), index=False)


def main(paths):
    Sweden(
        source_url=(
            "https://www.folkhalsomyndigheten.se/smittskydd-beredskap/utbrott/aktuella-utbrott/covid-19/"
            "vaccination-mot-covid-19/statistik/statistik-over-registrerade-vaccinationer-covid-19/"
        ),
        location="Sweden"
    ).to_csv(paths)


if __name__ == '__main__':
    main(output_dir)
