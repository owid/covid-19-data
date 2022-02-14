import re
import json

from bs4 import BeautifulSoup, element
import pandas as pd

from cowidev.utils import clean_count, clean_date_series, get_soup
from cowidev.testing.utils.base import CountryTestBase


class Andorra(CountryTestBase):
    location = "Andorra"
    units = "tests performed"
    source_label = "Tauler COVID-19, Govern d'Andorra"
    source_url_ref = "https://covid19.govern.ad"
    regex = {
        "script": r"'n_serologics': {",
        "pcr": r"'n_pcr': { type: 'line', data: { labels:(.*?), datasets: .*? data: (.*?), fill:",
        "tma": r"'n_tma': { type: 'line', data: { labels:(.*?), datasets: .*? data: (.*?), fill:",
    }

    def read(self) -> pd.DataFrame:
        """Reads data from the source page."""
        soup = get_soup(self.source_url_ref)
        data = self._parse_data(soup)
        return data

    def _parse_data(self, soup: BeautifulSoup) -> pd.DataFrame:
        """Gets data from the source page."""
        # Get relevant element
        elem = self._get_relevant_element(soup)
        # Extract text from element
        text = self._get_text_from_element(elem)
        # Extract data from text
        data = self._parse_metrics(text)
        return data

    def _get_relevant_element(self, soup: BeautifulSoup) -> element.Tag:
        """Gets the relevant element."""
        elem = soup.find("script", text=re.compile(self.regex["script"]))
        if not elem:
            raise ValueError("No element found, please update the script")
        return elem

    def _get_text_from_element(self, elem: element.Tag) -> str:
        """Extracts text from the element."""
        text = re.sub(r"\s+", " ", str(elem))
        return text

    def _parse_metrics(self, text: str) -> pd.DataFrame:
        """Get metrics from text."""
        df_pcr = self._df_builder("pcr", text)
        df_tma = self._df_builder("tma", text)
        df = pd.merge(df_pcr, df_tma)
        return df

    def _df_builder(self, regex_key: str, text: str) -> pd.DataFrame:
        """Builds Dataframe"""
        match = re.search(self.regex[regex_key], text)
        if not match:
            raise ValueError("No match found, please update the regex")
        df = pd.DataFrame([json.loads(match.group(1)), json.loads(match.group(2))], index=["Date", f"{regex_key}"]).T
        return df

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pipes date column."""
        return df.assign(Date=clean_date_series(df.Date, "%d/%m/%y"))

    def pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pipes metrics."""
        return df.assign(
            **{
                "Cumulative total": df.pcr.apply(clean_count) + df.tma.apply(clean_count),
            }
        )

    def pipe_correct_dp(self, df: pd.DataFrame):
        """Pipes the replacement data point."""
        date = "2021-03-22"
        correct_dp = 164665
        df.loc[df.Date == date, "Cumulative total"] = correct_dp
        return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pipeline for data."""
        return (
            df.pipe(self.pipe_date)
            .pipe(self.pipe_metrics)
            .pipe(self.pipe_correct_dp)
            .pipe(self.pipe_metadata)
            .sort_values("Date")
        )

    def export(self):
        """Exports data to CSV."""
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df)


def main():
    Andorra().export()
