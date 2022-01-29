import re
import json
from datetime import datetime, timedelta

import pandas as pd

from cowidev.testing import CountryTestBase
from cowidev.utils.web import get_soup
from cowidev.utils.clean import clean_date, clean_count


class ElSalvador(CountryTestBase):
    location: str = "El Salvador"
    units: str = "tests performed"
    source_label: str = "Government of El Salvador"
    source_url_ref: str = "https://covid19.gob.sv/"
    source_url: str = "https://e.infogram.com/"
    regex: dict = {
        "title": r"\'PRUEBAS REALIZADAS\'\, \'CASOS POSITIVOS\'",
        "element": r"window\.infographicData=({.*})",
    }
    rename_columns: dict = {
        "CASOS POSITIVOS": "positive",
        "PRUEBAS REALIZADAS": "Daily change in cumulative total",
    }

    def read(self) -> pd.DataFrame:
        """Read data from source"""
        data_id = self._get_data_id_from_source(self.source_url_ref)
        df = self._load_data(data_id)
        return df

    def _get_data_id_from_source(self, source_url: str) -> str:
        """Get Data ID from source"""
        soup = get_soup(source_url)
        data_id = soup.find(class_="infogram-embed")["data-id"]
        return data_id

    def _load_data(self, data_id: str) -> pd.DataFrame:
        """Load data from source"""
        url = f"{self.source_url}{data_id}"
        soup = get_soup(url)
        match = re.search(self.regex["element"], str(soup))
        if not match:
            raise ValueError("Website Structure Changed, please update the script")
        data = json.loads(match.group(1))
        data = data["elements"]["content"]["content"]["entities"]
        data = [data[idx] for idx in data if re.search(self.regex["title"], str(data[idx].values()))][0]
        data_list = data["props"]["chartData"]["data"]
        df = pd.DataFrame()
        for frame in data_list:
            col = frame.pop(0)
            col[0] = "Date"
            df = df.append(pd.DataFrame(frame, columns=col), ignore_index=True)
        return df

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean date"""
        last_date = clean_date(
            df[-1:]["Date"].item() + "-" + str(datetime.today().year), "%d-%b-%Y", lang="es", as_datetime=True
        )
        first_date = last_date - timedelta(len(df.index) - 1)
        df["Date"] = pd.Series(pd.date_range(first_date, last_date).astype(str))
        return df

    def pipe_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean numeric columns"""
        df["positive"] = df["positive"].apply(clean_count)
        df["Daily change in cumulative total"] = df["Daily change in cumulative total"].apply(clean_count)
        return df

    def pipe_pr(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate Positive Rate"""
        df["Positive rate"] = (
            df["positive"].rolling(7).sum().div(df["Daily change in cumulative total"].rolling(7).sum()).round(5)
        ).fillna(0)
        return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pipeline for data processing"""
        return (
            df.pipe(self.pipe_rename_columns)
            .pipe(self.pipe_date)
            .pipe(self.pipe_numeric)
            .pipe(self.pipe_pr)
            .pipe(self.pipe_metadata)
        )

    def export(self):
        """Export data to csv"""
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df, float_format="%.5f")


def main():
    ElSalvador().export()


if __name__ == "__main__":
    main()
