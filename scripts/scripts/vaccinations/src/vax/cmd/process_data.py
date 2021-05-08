import os

import pandas as pd

from vax.utils.gsheets import GSheet
from vax.process import process_location


def main_process_data(paths, google_credentials: str, google_spreadsheet_vax_id: str, skip_complete: list = None,
                      skip_monotonic: list = None):
    print("-- Processing data... --")
    # Get data from sheets
    print(">> Getting data from Google Spreadsheet...")
    gsheet = GSheet(
        google_credentials,
        google_spreadsheet_vax_id
    )
    df_manual_list = gsheet.df_list()

    # Get automated-country data
    print(">> Getting data from output...")
    automated = gsheet.automated_countries
    filepaths_auto = [paths.tmp_vax_loc(country) for country in automated]
    df_auto_list = [pd.read_csv(filepath) for filepath in filepaths_auto]

    # Concatenate
    vax = df_manual_list + df_auto_list

    # Process locations
    def _process_location(df):
        check = False if df.loc[0, "location"] in skip_monotonic else True
        return process_location(df, check)

    print(">> Processing and exporting data...")
    vax = [
        _process_location(df) for df in vax if df.loc[0, "location"].lower() not in skip_complete
    ]

    # Export
    for df in vax:
        country = df.loc[0, "location"]
        df.to_csv(paths.pub_vax_loc(country), index=False)
    df = pd.concat(vax).sort_values(by=["location", "date"])
    df.to_csv(paths.tmp_vax_all, index=False)
    gsheet.metadata.to_csv(paths.tmp_met_all, index=False)
    print(">> Exported")
    print("----------------------------\n----------------------------\n----------------------------\n")
