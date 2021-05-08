import argparse

from vax.cmd import main_get_data, main_process_data
from vax.cmd.get_data import modules_name, modules_name_batch, modules_name_incremental, country_to_module


def _parse_args():
    def _countries_to_modules(s):
        if s == "all":
            return modules_name
        elif s == "incremental":
            return modules_name_batch
        elif s == "batch":
            return modules_name_incremental
        # Comma separated string to list of strings
        countries = [ss.strip().replace(" ", "_").lower() for ss in s.split(",")]
        # Verify validity of countries
        countries_wrong = [c for c in countries if c not in country_to_module]
        if countries_wrong:
            print(f"Invalid countries: {countries_wrong}. Valid countries are: {list(country_to_module.keys())}")
            raise ValueError("Invalid country")
        # Get module equivalent names
        modules = [country_to_module[country] for country in countries]
        return modules

    parser = argparse.ArgumentParser(description="Execute COVID-19 vaccination data collection pipeline.")
    parser.add_argument(
        "mode", choices=["get-data", "process-data", "all"], default="all",
        help=(
            "Choose a step: i) get-data will run automated scripts, 2) process-data will get csvs generated in 1 and"
            "collect all data from spreadsheet, 3) will run both sequentially."
        )
    )
    parser.add_argument(
        "-c", "--countries", type=_countries_to_modules, default="all",
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
    args = parser.parse_args()
    return args


def main():
    args = _parse_args()
    if args.mode == "get-data":
        main_get_data(args.parallel, args.njobs, args.countries)
    elif args.mode == "process-data":
        main_process_data()
    elif args.mode == "all":
        main_get_data(args.parallel, args.njobs, args.countries)
        main_process_data()


if __name__ == "__main__":
    main()
