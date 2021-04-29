import os
import argparse

from vax.tracking.countries import countries_missing, country_updates_summary
from vax.tracking.vaccines import vaccines_missing


def _parse_args():
    parser = argparse.ArgumentParser(description="Get tracking information from dataset.")
    parser.add_argument(
        "mode", choices=[
            "countries-missing",
            "countries-last-updated",
            "countries-least-updated",
            "vaccines-missing",
        ],
        default="countries-last-updated",
        help=(
            "Choose a step: i) countries-missing will get table with countries not included in dataset, 2)"
            "countries-last-updated will get table with countries last updated, 3) countries-least-updated will"
            "get table with countries least updated, 4) vaccines-missing will get table with missing vaccines."
            "Unapproved (but tracked) and Untracked (but approved)."
        )
    )
    parser.add_argument(
        "--to-csv", action="store_true",
        help="Export outputs to CSV."
    )
    args = parser.parse_args()
    return args


def export_to_csv(df, filename):
    df.to_csv(filename, index=False)
    filename = os.path.abspath(filename)
    print(f"Data exported to {filename}")


def main():
    args = _parse_args()
    if args.mode == "countries-missing":
        print("-- Missing countries... --\n")
        df = countries_missing()
        print(df)
        print("----------------------------\n----------------------------\n----------------------------\n")
        if args.to_csv:
            export_to_csv(df, filename="countries-missing.tmp.csv")
    if args.mode == "countries-last-updated":
        print("-- Last updated countries... --")
        df = country_updates_summary()
        print(df)
        print("----------------------------\n----------------------------\n----------------------------\n")
        if args.to_csv:
            export_to_csv(df, filename="countries-last-updated.tmp.csv")
    if args.mode == "countries-least-updated":
        print("-- Least updated countries... --")
        df = country_updates_summary(sortby_counts=True)
        print(df)
        print("----------------------------\n----------------------------\n----------------------------\n")
        if args.to_csv:
            export_to_csv(df, filename="countries-least-updated.tmp.csv")
    if args.mode == "vaccines-missing":
        print("-- Missing vaccines... --")
        df = vaccines_missing(verbose=True)
        print(df)
        print("----------------------------\n----------------------------\n----------------------------\n")
        if args.to_csv:
            export_to_csv(df, filename="vaccines-missing.tmp.csv")


if __name__ == "__main__":
    main()
