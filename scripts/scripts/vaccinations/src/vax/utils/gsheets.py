import tempfile
import os
from typing import List
import json


import pandas as pd
from gsheets import Sheets

from vax.utils.checks import country_df_sanity_checks


class GSheet:
    def __init__(self, credentials: str, sheet_id: str):
        self.sheet_id = sheet_id
        self.sheets = Sheets.from_files(credentials)
        self.sheet = self.sheets.get(self.sheet_id)
        self.metadata = self.get_metadata()

    @classmethod
    def from_json(cls, path: str):
        """Load sheet from config file."""
        with open(path, "rb") as f:
            conf = json.load(f)
        return cls(
            credentials=conf["google_credentials"],
            sheet_id=conf["google_spreadsheet_vax_id"]
        )

    def get_metadata(self, refresh: bool = False) -> pd.DataFrame:
        """Get metadata from LOCATIONS tab."""
        if refresh:
            self.sheet = self.sheets.get(self.sheet_id)
        metadata = self.sheet.first_sheet.to_frame()
        self._check_metadata(metadata)
        metadata = metadata[metadata["include"]].sort_values(by="location")
        return metadata

    def _check_metadata(self, df: pd.DataFrame):
        """Check metadata LOCATIONS tab has valid format."""
        # Check columns
        cols = ["location", "source_name", "automated", "include"]
        cols_missing = [col for col in cols if col not in df.columns]
        cols_wrong = [col for col in df.columns if col not in cols]
        if cols_missing:
            raise ValueError(f"LOCATIONS missing column(s): {cols_missing}.")
        if cols_wrong:
            raise ValueError(f"LOCATIONS has invalid column(s): {cols_wrong}.")
        # Check duplicated rows
        location_counts = df.location.value_counts()
        if (location_counts > 1).any(None):
            locations_dup = location_counts[location_counts > 1].index.tolist()
            raise ValueError(f"Duplicated location(s) found in LOCATIONS. Check {locations_dup}")
        if df.isnull().any(None):
            raise ValueError("Check LOCATIONS. Some fields missing (empty / NaNs)")
        # Ensure booleanity of columns automated, include
        if not df.automated.isin([True, False]).all():
            vals = df.automated.unique()
            raise ValueError(f"LOCATIONS column `automated` should only contain TRUE/FALSE. Check {vals}")
        if not df.include.isin([True, False]).all():
            vals = df.include.unique()
            raise ValueError(f"LOCATIONS column `include` should only contain TRUE/FALSE. Check {vals}")

    @property
    def automated_countries(self):
        """Get list of countries with an automated process."""
        return self.metadata.loc[self.metadata.automated, "location"].tolist()

    @property
    def manual_countries(self):
        """Get list of countries that require manual process."""
        return self.metadata.loc[~self.metadata.automated, "location"].tolist()

    def df_list(self, include_all: bool = False, refresh: bool = False) -> List[pd.DataFrame]:
        """Read non-automated files.

        Args:
            include_all (bool): Set to True to only load non-automated countries.
            refresh (bool): Set to True to get updated data from sheets.

        Returns:
            List[pd.DataFrame]: List with dataframe per country.
        """
        if refresh:
            self.sheet = self.sheets.get(self.sheet_id)
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
            df_list = [read_csv_and_check(filepath) for filepath in filepaths]
        self._check_with_metadata(df_list, filepaths)
        return df_list

    def _check_with_metadata(self, df_list: list, filepaths: list):
        """Checks if country tabs are aligned with LOCATIONS metadata tab content."""
        # Assumes df has location field, which only presents a value
        tab_locations = [df.location.unique()[0] for df in df_list]
        counts = pd.value_counts(tab_locations)
        if (counts > 1).any(None):
            duplicated_tabs = counts[counts > 1].index.tolist()
            raise ValueError(f"Duplicated location(s)! Location(s) {duplicated_tabs} were found in multiple tabs.")
        # Find missing tabs / missing entries
        metadata_missing = [loc for loc in tab_locations if loc not in self.manual_countries]
        tab_missing = [loc for loc in self.manual_countries if loc not in tab_locations]
        error_msg = []
        if metadata_missing:
            error_msg.append(f"Tab containing a location missing in LOCATIONS: {str(metadata_missing)}")
        if tab_missing:
            error_msg.append(f"Location found in LOCATIONS but no tab with such location was found: "
                             f"{str(tab_missing)}")
        if error_msg:
            error_msg = "\n".join(error_msg)
            raise ValueError(error_msg)


def read_csv_and_check(filepath):
    # Read
    try:
        df = pd.read_csv(
            filepath,
            # parse_dates=["date"],
            # dayfirst=True
        )
    except Exception:
        raise ValueError(f"Check the spreadsheet corresponding to {filepath}")
    location = df.loc[:, "location"].unique()
    # Date check
    try:
        df = df.assign(date=pd.to_datetime(df["date"], format="%Y-%m-%d"))
    except Exception:
        raise ValueError(f"{location} -- Invalid date format! Should be %Y-%m-%d. Check {df.date}.")
    if not df.date.is_monotonic:
        raise ValueError(f"{location} -- Check that date field is monotonically increasing.")
    # Checks
    country_df_sanity_checks(df)
    return df
