import os
import argparse

from vax.tracking.countries import countries_missing, country_updates_summary
from vax.tracking.vaccines import vaccines_missing


def _parse_args():
    parser = argparse.ArgumentParser(description="Get tracking information from dataset.")
    parser.add_argument(
        "--countries-missing", action="store_true",
        help="Get table with countries not included in dataset."
    )
    parser.add_argument(
        "--countries-last-updated", action="store_true",
        help="Get table with countries last updated."
    )
    parser.add_argument(
        "--countries-least-updated", action="store_true",
        help="Get table with countries least updated."
    )
    parser.add_argument(
        "--vaccines-missing", action="store_true",
        help="Get table with missing vaccines. Unapproved (but tracked) and Untracked (but approved)."
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
    if args.countries_missing:
        print("-- Missing countries... --\n")
        df = countries_missing()
        print(df)
        print("----------------------------\n----------------------------\n----------------------------\n")
        if args.to_csv:
            export_to_csv(df, filename="countries-missing.tmp.csv")
    if args.countries_last_updated:
        print("-- Last updated countries... --")
        df = country_updates_summary()
        print(df)
        print("----------------------------\n----------------------------\n----------------------------\n")
        if args.to_csv:
            export_to_csv(df, filename="countries-last-updated.tmp.csv")
    if args.countries_least_updated:
        print("-- Least updated countries... --")
        df = country_updates_summary(sortby_counts=True)
        print(df)
        print("----------------------------\n----------------------------\n----------------------------\n")
        if args.to_csv:
            export_to_csv(df, filename="countries-least-updated.tmp.csv")
    if args.vaccines_missing:
        print("-- Missing vaccines... --")
        df = vaccines_missing(verbose=True)
        print(df)
        print("----------------------------\n----------------------------\n----------------------------\n")
        if args.to_csv:
            export_to_csv(df, filename="vaccines-missing.tmp.csv")
    if not any([args.countries_missing, args.countries_last_updated, args.countries_least_updated,
                args.vaccines_missing]):
        print(
            "[!] Choose an option from --countries-missing, --countries-last-updated, --countries-least-updated"
            "or --vaccines-missing"
        )


if __name__ == "__main__":
    main()