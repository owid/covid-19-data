import tempfile
import os
from typing import List
import json


import pandas as pd
import gsheets
from gsheets import Sheets


class GSheet:
    def __init__(self, credentials: str, sheet_id: str):
        self.sheets = Sheets.from_files(credentials)
        self.sheet = self.sheets.get(sheet_id)
        self.metadata = self.get_metadata()

    @classmethod
    def from_json(cls, path: str):
        with open(path, "rb") as f:
            conf = json.load(f)
        return cls(
            credentials=conf["google_credentials"],
            sheet_id=conf["google_spreadsheet_vax_id"]
        )

    def get_metadata(self) -> pd.DataFrame:
        metadata = self.sheet.first_sheet.to_frame()
        metadata = metadata[metadata["include"]].sort_values(by="location")
        return metadata

    @property
    def automated_countries(self):
        return self.metadata.loc[self.metadata.automated, "location"].tolist()

    def df_list(self, include_all: bool = False) -> List[pd.DataFrame]:
        """Read non-automated files.
        
        Args: 
            include_all (bool): Set to True to only load non-automated countries.

        Returns:
            List[pd.DataFrame]: List with dataframe per country.
        """
        with tempfile.TemporaryDirectory() as tmpdirname:
            # Download
            tmpfile = os.path.join(tmpdirname, "%(sheet)s.csv")
            self.sheet.to_csv(make_filename=tmpfile)
            # Read
            all_files = os.listdir(tmpdirname)
            if include_all:
                filepaths = [os.path.join(tmpdirname, filepath) for filepath in all_files]
            else:
                exclude = ["LOCATIONS.csv"] + [f"{country}.csv" for country in self.automated_countries]
                filepaths = [os.path.join(tmpdirname, filepath) for filepath in all_files if filepath not in exclude]
            df_list = [pd.read_csv(filepath) for filepath in filepaths]
        return df_list
