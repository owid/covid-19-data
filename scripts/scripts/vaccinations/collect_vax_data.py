"""Generate dataset files.

- Retrieves data from the Google Spreadsheet and automations/output folder
- Generates country csv files and vaccination & metadata temporary files (used & removed by generate_dataset.R)
"""
import tempfile
import os
from datetime import datetime
from typing import List
import json

import pandas as pd
import gsheets
from gsheets import Sheets


CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
AUTOMATED_OUTPUT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "automations/output"))
PUBLIC_DATA_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "../../../public/data/vaccinations/country_data/"))
CONFIG_FILE = os.path.abspath(os.path.join(CURRENT_DIR, "vax_dataset_config.json"))
SKIP_COUNTRIES = []


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


def process_location(df: pd.DataFrame) -> pd.DataFrame:
    # Only report up to previous day to avoid partial reporting
    df = df.assign(date=pd.to_datetime(df.date, dayfirst=True))
    df = df[df.date.dt.date < datetime.now().date()] 
    # Default columns for second doses
    if "people_vaccinated" not in df:
        df = df.assign(people_vaccinated=pd.NA)
    if "people_fully_vaccinated" not in df:
        df = df.assign(people_fully_vaccinated=pd.NA)
    # Avoid decimals
    cols = ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]
    df[cols] = df[cols].astype("Int64").fillna(pd.NA)
    # Order columns and rows
    usecols = [
        "location", "date", "vaccine", "source_url", "total_vaccinations", "people_vaccinated",
        "people_fully_vaccinated"
    ]
    df = df[usecols]
    df = df.sort_values(by="date")
    # Sanity checks
    _sanity_checks(df)
    # Strip
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    # Date format
    df = df.assign(date=df.date.dt.strftime("%Y-%m-%d"))

    return df


def _sanity_checks(df: pd.DataFrame) -> pd.DataFrame:
    location = df.loc[:, "location"].unique()
    vaccines_accepted = [
        "Pfizer/BioNTech", "Moderna", "Oxford/AstraZeneca", "Sputnik V", "Sinopharm/Beijing",
        "Sinopharm/Wuhan", "Johnson&Johnson", "Sinovac", "Covaxin", "EpiVacCorona"
    ]
    df_ = df[["people_vaccinated", "people_fully_vaccinated", "total_vaccinations"]].dropna()
    vaccines_used = set([xx for x in df.vaccine.tolist() for xx in x.split(', ')])
    if not all([vac in vaccines_accepted for vac in vaccines_used]):
        raise ValueError(f"{location} -- Invalid vaccine detected! Check {df.vaccine.unique()}")
    if (df.date.min() < datetime(2020, 12, 1)) or (df.date.max() > datetime.now().date()):
        raise ValueError(f"{location} -- Invalid dates! Check {df.date.min()} and {df.date.max()}")
    if any(df.location.isnull()) or df.location.nunique() != 1:
        raise ValueError(f"{location} -- Invalid location! Check {df.location}")
    if df.date.nunique() != len(df):
        raise ValueError(f"{location} -- Missmatch between number of rows {len(df)} and number of different dates"
                            f"{df.date.nunique()}. Check {df.date.unique()}")
    if any(df_.people_fully_vaccinated > df_.people_vaccinated) or any(df_.people_vaccinated > df_.total_vaccinations):
        raise ValueError(f"{location} -- Logic not valid! Check columns ['people_vaccinated', "
                          "'people_fully_vaccinated', 'total_vaccinations']")


if __name__ == "__main__":

    # Get data from sheets
    print("Getting data from Google Spreadsheet...")
    gsheet = GSheet.from_json(path=CONFIG_FILE)
    df_manual_list = gsheet.df_list()

    # Get automated-country data
    print("Getting data from automations/output...")
    automated = gsheet.automated_countries
    filepaths_auto = [os.path.join(AUTOMATED_OUTPUT_DIR, f"{country}.csv") for country in automated]
    df_auto_list = [pd.read_csv(filepath) for filepath in filepaths_auto]

    # Concatenate
    vax = df_manual_list + df_auto_list

    # Process locations
    print("Processing and exporting data...")
    vax = [process_location(df) for df in vax if df.loc[0, "location"] not in SKIP_COUNTRIES]

    # Export
    for df in vax:
        country = df.loc[0, "location"]
        df.to_csv(os.path.join(PUBLIC_DATA_DIR, f"{country}.csv"), index=False)
    df = pd.concat(vax).sort_values(by=["location", "date"])
    df.to_csv("vaccinations.preliminary.csv", index=False)
    gsheet.metadata.to_csv("metadata.preliminary.csv", index=False)
    print("Exported")
