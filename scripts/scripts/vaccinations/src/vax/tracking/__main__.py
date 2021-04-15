import argparse

from vax.tracking.countries import missing_countries, country_updates_summary
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


def main():
    args = _parse_args()
    if args.countries_missing:
        print("-- Missing countries... --\n")
        df = countries_missing()
        if args.to_csv:
            df.to_csv("countries-missing.csv", index=False)
        print(df)
        print("----------------------------\n----------------------------\n----------------------------\n")
    if args.countries_last_updated:
        print("-- Last updated countries... --")
        df = country_updates_summary()
        if args.to_csv:
            df.to_csv("countries-last-updated.csv", index=False)
        print(df)
        print("----------------------------\n----------------------------\n----------------------------\n")
    if args.countries_least_updated:
        print("-- Least updated countries... --")
        df = country_updates_summary(sortby_counts=True)
        if args.to_csv:
            df.to_csv("countries-least-updated.csv", index=False)
        print(df)
        print("----------------------------\n----------------------------\n----------------------------\n")
    if args.vaccines_missing:
        print("-- Missing vaccines... --")
        df = vaccines_missing(verbose=True)
        if args.to_csv:
            df.to_csv("vaccines-missing.csv", index=False)
        print(df)
        print("----------------------------\n----------------------------\n----------------------------\n")
    if not any([args.countries_missing, args.countries_last_updated, args.countries_least_updated,
                args.vaccines_missing]):
        print(
            "[!] Choose an option from --countries-missing, --countries-last-updated, --countries-least-updated"
            "or --vaccines-missing"
        )


if __name__ == "__main__":
    main()