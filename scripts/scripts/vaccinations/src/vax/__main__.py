"""
from joblib import Parallel, delayed
def process(i):
    return i * i
    
results = Parallel(n_jobs=2)(delayed(process)(i) for i in range(10))
print(results)
"""
import argparse
import logging
import os
import importlib
from datetime import datetime

import pandas as pd

from vax.batch import __all__ as batch_countries
from vax.incremental import __all__ as incremental_countries
from vax.utils.gsheets import GSheet
from vax.process import process_location


SCRAPING_SKIP_COUNTRIES = []
PROCESS_SKIP_COUNTRIES = []
VAX_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))

AUTOMATED_OUTPUT_DIR = os.path.abspath(os.path.join(VAX_ROOT_DIR, "./output"))
PUBLIC_DATA_DIR = os.path.abspath(os.path.join(VAX_ROOT_DIR, "../../../public/data/vaccinations/country_data/"))
CONFIG_FILE = os.path.abspath(os.path.join(VAX_ROOT_DIR, "vax_dataset_config.json"))


logger = logging.Logger('catch_all')

batch_countries = [f"vax.batch.{c}" for c in batch_countries]
incremental_countries = [f"vax.incremental.{c}" for c in incremental_countries]
modules_name = batch_countries + incremental_countries


def main_get_data():
    """Get data from sources and export to output folder.
    
    Is equivalent to script `run_python_scripts.py`
    """
    modules_failed = []
    for module_name in modules_name:
        date_str = datetime.now().strftime("%Y-%m-%d %X")
        print(f">> {date_str} - {module_name}")
        country = module_name.split(".")[-1]
        if country in SCRAPING_SKIP_COUNTRIES:
            print("    skipped!")
            continue
        module = importlib.import_module(module_name)
        try:
            module.main()
        except Exception as err:
            logger.error(err, exc_info=True)
            modules_failed.append(module_name)
            print()

    # Retry failed modules
    print(f"\n---\n\nRETRIALS ({len(modules_failed)})")
    for module_name in modules_failed:
        date_str = datetime.now().strftime("%Y-%m-%d %X")
        print(f">> {date_str} - {module_name} - (RETRIAL)")
        module = importlib.import_module(module_name)
        try:
            module.main()
        except Exception as err:
            logger.error(err, exc_info=True)
            print()

    if len(modules_failed) > 0:
        print(f"\n---\n\nThe following scripts failed to run ({len(modules_failed)}):")
        print("\n".join([f"* {m}" for m in modules_failed]))


def main_process_data():
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
    print(">> Processing and exporting data...")
    vax = [process_location(df) for df in vax if df.loc[0, "location"] not in PROCESS_SKIP_COUNTRIES]

    # Export
    for df in vax:
        country = df.loc[0, "location"]
        df.to_csv(os.path.join(PUBLIC_DATA_DIR, f"{country}.csv"), index=False)
    df = pd.concat(vax).sort_values(by=["location", "date"])
    df.to_csv("vaccinations.preliminary.csv", index=False)
    gsheet.metadata.to_csv("metadata.preliminary.csv", index=False)
    print(">> Exported")


def _parse_args():
    parser = argparse.ArgumentParser(description="Execute data collection pipeline.")
    parser.add_argument(
        "-ng", "--no-get-data", action="store_true", type=bool
        help="Skip getting the data."
    )
    parser.add_argument(
        "-np", "--no-process-data", action="store_true", type=bool,
        help="Skip processing the data."
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = _parse_args()
    print(args.no_get_data)
    print(args.no_process_data)
    if not args.no_get_data:
        main_get_data()
        print("----------------------------\n----------------------------\n----------------------------\n")
    if not args.no_process_data:
        main_process_data()
        print("----------------------------\n----------------------------\n----------------------------\n")