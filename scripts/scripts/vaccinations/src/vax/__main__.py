import argparse
import logging
import os
import importlib
from datetime import datetime

import pandas as pd
from joblib import Parallel, delayed

from vax.batch import __all__ as batch_countries
from vax.incremental import __all__ as incremental_countries
from vax.utils.gsheets import GSheet
from vax.process import process_location

# Variables
SCRAPING_SKIP_COUNTRIES = []
PROCESS_SKIP_COUNTRIES = []
SKIP_COUNTRIES_MONOTONIC_CHECK = ["Northern Ireland", "Malta"]

SCRAPING_SKIP_COUNTRIES = [x.lower() for x in SCRAPING_SKIP_COUNTRIES]
PROCESS_SKIP_COUNTRIES = [x.lower() for x in PROCESS_SKIP_COUNTRIES]

# Directories
VAX_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
AUTOMATED_OUTPUT_DIR = os.path.abspath(os.path.join(VAX_ROOT_DIR, "./output"))
PUBLIC_DATA_DIR = os.path.abspath(os.path.join(VAX_ROOT_DIR, "../../../public/data/vaccinations/country_data/"))
CONFIG_FILE = os.path.abspath(os.path.join(VAX_ROOT_DIR, "vax_dataset_config.json"))

# Logging config
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()

# Import modules
batch_countries = [f"vax.batch.{c}" for c in batch_countries]
incremental_countries = [f"vax.incremental.{c}" for c in incremental_countries]
modules_name = batch_countries + incremental_countries


def _get_data_country(module_name):
    country = module_name.split(".")[-1]
    if country.lower() in SCRAPING_SKIP_COUNTRIES:
        logger.info(f"{module_name} skipped!")
        return {
            "module_name": module_name,
            "success": None,
            "skipped": True
        }
    logger.info(f"{module_name}: started")
    module = importlib.import_module(module_name)
    try:
        module.main()
    except Exception as err:
        success = False
        logger.error(f"{module_name}: {err}", exc_info=True)
    else:
        success = True
        logger.info(f"{module_name}: SUCCESS")
    return {
        "module_name": module_name,
        "success": success,
        "skipped": False
    }


def main_get_data(parallel: bool = False, n_jobs: int = -2):
    """Get data from sources and export to output folder.

    Is equivalent to script `run_python_scripts.py`
    """
    print("-- Getting data... --")
    if parallel:
        modules_execution_results = Parallel(n_jobs=n_jobs, backend="threading")(
            delayed(_get_data_country)(module_name) for module_name in modules_name
        )
    else:
        modules_execution_results = []
        for module_name in modules_name:
            modules_execution_results.append(_get_data_country(module_name))

    modules_failed = [m["module_name"] for m in modules_execution_results if m["success"] is False]
    # Retry failed modules
    print(f"\n---\n\nRETRIALS ({len(modules_failed)})")
    modules_failed_retrial = []
    for module_name in modules_failed:
        date_str = datetime.now().strftime("%Y-%m-%d %X")
        print(f">> {date_str} - {module_name} - (RETRIAL)")
        module = importlib.import_module(module_name)
        try:
            module.main()
        except Exception as err:
            modules_failed_retrial.append(module)
            logger.error(err, exc_info=True)
            print()

    if len(modules_failed_retrial) > 0:
        print(f"\n---\n\nThe following scripts failed to run ({len(modules_failed_retrial)}):")
        print("\n".join([f"* {m}" for m in modules_failed_retrial]))
    print("----------------------------\n----------------------------\n----------------------------\n")


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


def _parse_args():
    parser = argparse.ArgumentParser(description="Execute COVID-19 vaccination data collection pipeline.")
    parser.add_argument(
        "mode", choices=["get-data", "process-data", "all"], default="all",
        help=(
            "Choose a step: i) get-data will run automated scripts, 2) process-data will get csvs generated in 1 and"
            "collect all data from spreadsheet, 3) will run both sequentially."
        )
    )
    parser.add_argument(
        "-p", "--parallel", action="store_true",
        help="Execution done in parallel (only in mode get-data)."
    )
    parser.add_argument(
        "-j", "--njobs", default=-2,
        help=(
            "Number of jobs for parallel processing. Check Parallel class in joblib library for more info  (only in ""
            "mode get-data)."
        )
    )
    args = parser.parse_args()
    return args


def main():
    args = _parse_args()
    if args.mode=="get-data":
        main_get_data(args.parallel, args.njobs)
    elif args.mode=="process-data":
        main_process_data()
    elif args.mode=="all":
        main_get_data(args.parallel, args.njobs)
        main_process_data()


if __name__ == "__main__":
    main()
