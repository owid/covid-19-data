import os

import pandas as pd

from vax.utils.gsheets import GSheet
from vax.process import process_location


PROCESS_SKIP_COUNTRIES = []
PROCESS_SKIP_COUNTRIES = [x.lower() for x in PROCESS_SKIP_COUNTRIES]
SKIP_COUNTRIES_MONOTONIC_CHECK = ["Northern Ireland", "Malta", "Romania", "Sweden"]

VAX_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
CONFIG_FILE = os.path.abspath(os.path.join(VAX_ROOT_DIR, "vax_dataset_config.json"))
AUTOMATED_OUTPUT_DIR = os.path.abspath(os.path.join(VAX_ROOT_DIR, "./output"))
PUBLIC_DATA_DIR = os.path.abspath(os.path.join(VAX_ROOT_DIR, "../../../public/data/vaccinations/country_data/"))


def main_process_data():
    print("-- Processing data... --")
    # Get data from sheets
    print(">> Getting data from Google Spreadsheet...")
    gsheet = GSheet.from_json(path=CONFIG_FILE)
    df_manual_list = gsheet.df_list()

    # Get automated-country data
    print(">> Getting data from output...")
    automated = gsheet.automated_countries
    filepaths_auto = [os.path.join(AUTOMATED_OUTPUT_DIR, f"{country}.csv") for country in automated]
    df_auto_list = [pd.read_csv(filepath) for filepath in filepaths_auto]

    # Concatenate
    vax = df_manual_list + df_auto_list

    # Process locations
    def _process_location(df):
        check = False if df.loc[0, "location"] in SKIP_COUNTRIES_MONOTONIC_CHECK else True
        return process_location(df, check)

    print(">> Processing and exporting data...")
    vax = [
        _process_location(df) for df in vax if df.loc[0, "location"].lower() not in PROCESS_SKIP_COUNTRIES
    ]

    # Export
    for df in vax:
        country = df.loc[0, "location"]
        df.to_csv(os.path.join(PUBLIC_DATA_DIR, f"{country}.csv"), index=False)
    df = pd.concat(vax).sort_values(by=["location", "date"])
    df.to_csv("vaccinations.preliminary.csv", index=False)
    gsheet.metadata.to_csv("metadata.preliminary.csv", index=False)
    print(">> Exported")
    print("----------------------------\n----------------------------\n----------------------------\n")
