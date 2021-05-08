import argparse
import os

from vax.cmd.get_data import modules_name, modules_name_batch, modules_name_incremental, country_to_module


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Execute COVID-19 vaccination data collection pipeline.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "mode", choices=["get-data", "process-data", "all"], default="all",
        help=(
            "Choose a step: i) get-data will run automated scripts, 2) process-data will get csvs generated in 1 and"
            "collect all data from spreadsheet, 3) will run both sequentially."
        )
    )
    parser.add_argument(
        "-c", "--countries", default="all",
        help=(
            "Run for a specific country. For a list of countries use commas to separate them (only in mode get-data)"
            "E.g.: peru, norway. \nSpecial keywords: 'all' to run all countries, 'incremental' to run incremental"
            "updates, 'batch' to run batch updates. Defaults to all countries."
        )
    )
    parser.add_argument(
        "-p", "--parallel", action="store_true",
        help="Execution done in parallel (only in mode get-data)."
    )
    parser.add_argument(
        "-j", "--njobs", default=-2,
        help=(
            "Number of jobs for parallel processing. Check Parallel class in joblib library for more info  (only in "
            "mode get-data)."
        )
    )
    parser.add_argument(
        "-s", "--show-config", action="store_true",
        help="Display configuration parameters at the beginning of the execution."
    )
    parser.add_argument(
        "--config", default=os.path.join(os.path.expanduser("~"), ".config", "cowid", "config.yaml"),
        help=(
            "Path to config file (YAML)."
        )
    )
    args = parser.parse_args()
    return args