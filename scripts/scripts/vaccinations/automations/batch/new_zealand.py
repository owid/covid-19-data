from datetime import datetime, timedelta
from urllib.parse import urlparse
import pandas as pd
from bs4 import BeautifulSoup
from utils import utils


class NewZealand(object):

    def __init__(self, source_url: str, location: str, columns_rename: dict = None, columns_cumsum: list = None):
        """Constructor

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
        self.columns_cumsum = columns_cumsum

    @property
    def output_file(self):
        return f"automations/output/{self.location}.csv"

    def load_data(self) -> pd.DataFrame:
        """Load original data"""
        soup = utils.get_soup(self.source_url)
        link = self._parse_file_link(soup)
        return utils.read_xlsx_from_url(link, sheet_name="Date")

    def _parse_file_link(self, soup: BeautifulSoup) -> str:
        href = soup.find(id="download").find_next("a")["href"]
        link = f"https://{urlparse(self.source_url).netloc}/{href}"
        return link

    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generalized"""
        if self.columns_rename:
            return df.rename(columns=self.columns_rename)
        return df

    def cumsum_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generalized"""
        if self.columns_cumsum:
            df[self.columns_cumsum] = df[self.columns_cumsum].cumsum()
        return df

    def add_totals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Could be generalized"""
        return df.assign(total_vaccinations=df.people_vaccinated+df.people_fully_vaccinated)

    def enrich_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        """Could be generalized"""
        return df.assign(vaccine="Pfizer/BioNTech")

    def enrich_location(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generalized"""
        return df.assign(location=self.location)

    def enrich_source_url(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generalized"""
        return df.assign(source_url=self.source_url)

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        """Could be generalized"""
        return (
            df
            .pipe(self.cumsum_columns)
            .pipe(self.rename_columns)
            .pipe(self.add_totals)
            .pipe(self.enrich_vaccine)
            .pipe(self.enrich_location)
            .pipe(self.enrich_source_url)
        )

    def to_csv(self, output_file: str = None):
        """Generalized"""
        df = self.load_data().pipe(self.pipeline)
        if output_file is None:
            output_file = self.output_file
        df.to_csv(output_file, index=False)


if __name__ == "__main__":
    df = NewZealand(
        source_url=(
            "https://www.health.govt.nz/our-work/diseases-and-conditions/covid-19-novel-coronavirus/covid-19-data-and-statistics/covid-19-vaccine-data"
        ),
        location="New Zealand",
        columns_rename={
            "First dose administered": "people_vaccinated",
            "Second dose administered": "people_fully_vaccinated",
            "Date": "date",
        },
        columns_cumsum=["First dose administered", "Second dose administered"]
    ).to_csv()
