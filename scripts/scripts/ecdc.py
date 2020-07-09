import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from termcolor import colored

CURRENT_DIR = os.path.dirname(__file__)
sys.path.append(CURRENT_DIR)

import megafile
from shared import load_population, load_owid_continents, inject_total_daily_cols, \
    inject_owid_aggregates, inject_per_million, inject_days_since, inject_cfr, inject_population, \
    inject_rolling_avg, inject_exemplars, inject_doubling_days, inject_weekly_growth, inject_biweekly_growth, standard_export

INPUT_PATH = os.path.join(CURRENT_DIR, '../input/ecdc/')
OUTPUT_PATH = os.path.join(CURRENT_DIR, '../../public/data/ecdc/')
TMP_PATH = os.path.join(CURRENT_DIR, '../tmp')

LOCATIONS_CSV_PATH = os.path.join(INPUT_PATH, 'ecdc_country_standardized.csv')
RELEASES_PATH = os.path.join(INPUT_PATH, 'releases')

ERROR = colored("[Error]", 'red')
WARNING = colored("[Warning]", 'yellow')

DATASET_NAME = "COVID-2019 - ECDC (2020)"

def print_err(*args, **kwargs):
    return print(*args, file=sys.stderr, **kwargs)

# Used to be there until 27 March 2020
# And back again from 28 March... :?
def download_xlsx(last_n=2):
    daterange = pd.date_range(end=datetime.utcnow(), periods=last_n).to_pydatetime().tolist()
    for date in daterange:
        filename = date.strftime('%Y-%m-%d')
        for ext in ['xlsx']:
            os.system('curl --silent -f -o %(DIR)s/%(filename)s https://www.ecdc.europa.eu/sites/default/files/documents/COVID-19-geographic-disbtribution-worldwide-%(filename)s' % {
                'filename': filename + '.' + ext,
                'DIR': RELEASES_PATH
            })

def download_csv():
    os.system('curl --silent -f -o %(DIR)s/latest.csv -L https://opendata.ecdc.europa.eu/covid19/casedistribution/csv' % {
        'DIR': RELEASES_PATH
    })

def read_file(filename):
    filepath = os.path.join(RELEASES_PATH, filename)
    if filepath.endswith("csv"):
        return pd.read_csv(
            filepath,
            # Namibia has 'NA' 2-letter code, we don't want that to be <NA>
            keep_default_na=False,
            na_values=[""],
            encoding="UTF-8"
        )
    else:
        return pd.read_excel(
            filepath,
            # Namibia has 'NA' 2-letter code, we don't want that to be <NA>
            keep_default_na=False,
            na_values=[""]
        )

def load_data(filename):
    df = read_file(filename)
    # set to ints
    df['cases'] = df['cases'].astype("Int64")
    df['deaths'] = df['deaths'].astype("Int64")
    df['dateRep'] = pd.to_datetime(df['dateRep'], format="%d/%m/%Y", utc=True)
    # fill time gaps
    df = df.set_index(['dateRep']) \
        .groupby('countriesAndTerritories', as_index=True) \
        .resample('D').first() \
        .drop(columns=['countriesAndTerritories']) \
        .reset_index()
    df['dateRep'] = df['dateRep'].dt.date
    return df

def load_locations():
    return pd.read_csv(
        LOCATIONS_CSV_PATH,
        keep_default_na=False
    ).rename(columns={
        'Country': 'countriesAndTerritories',
        'Our World In Data Name': 'location'
    })

def _load_merged(filename):
    df_data = load_data(filename)
    df_locs = load_locations()
    return df_data.merge(
        df_locs,
        how='left',
        on=['countriesAndTerritories']
    )

def check_data_correctness(filename):
    errors = 0
    df_merged = _load_merged(filename)
    df_uniq = df_merged[['countriesAndTerritories', 'geoId', 'location']].drop_duplicates()
    if df_uniq['location'].isnull().any():
        print_err("\n" + ERROR + " Could not find OWID names for:")
        print_err(df_uniq[df_uniq['location'].isnull()])
        csv_path = os.path.join(TMP_PATH, 'ecdc.csv')
        os.system('mkdir -p %s' % os.path.abspath(TMP_PATH))
        df_uniq[['countriesAndTerritories']] \
            .drop_duplicates() \
            .rename(columns={'countriesAndTerritories': 'Country'}) \
            .to_csv(csv_path, index=False)
        print_err("\nSaved CSV file to be standardized at %s. \nRun it through the OWID standardizer and save in %s" % (
            colored(os.path.abspath(csv_path), 'magenta'),
            colored(os.path.abspath(LOCATIONS_CSV_PATH), 'magenta')
        ))
        errors += 1
    # Drop missing locations for the further checks – that error is addressed above
    df_merged = df_merged.dropna(subset=['location'])
    if df_merged.duplicated(subset=['dateRep', 'location']).any():
        print_err("\n" + ERROR + " Found duplicate rows:")
        print_err(df_merged[df_merged.duplicated(subset=['dateRep', 'location'])])
        print_err("\nPlease " + colored("fix or remove the duplicate rows", 'magenta') + " in the Excel file, and then save it again but under a new name, e.g. 2020-03-20-modified.xlsx")
        print_err("Also please " + colored("note down any changes you made", 'magenta') + " in %s" % os.path.abspath(os.path.join(INPUT_PATH, 'NOTES.md')))
        errors += 1
    df_pop = load_population()
    pop_entity_diff = set(df_uniq['location']) - set(df_pop['location'])
    if len(pop_entity_diff) > 0:
        # this is not an error, so don't increment errors variable
        print("\n" + WARNING + " These entities were not found in the population dataset:")
        print(pop_entity_diff)
        print()
    return True if errors == 0 else False

def discard_rows(df):
    # df.loc[(df['location'] == 'Spain') & (df['new_cases'] < 0), 'new_cases'] = pd.NA
    return df

# Must output columns:
# date, location, new_cases, new_deaths, total_cases, total_deaths
def load_standardized(filename):
    df = _load_merged(filename) \
        .drop(columns=[
            'countriesAndTerritories', 'geoId',
            'day', 'month', 'year',
        ]) \
        .rename(columns={
            'dateRep': 'date',
            'cases': 'new_cases',
            'deaths': 'new_deaths'
        })
    df = df[['date', 'location', 'new_cases', 'new_deaths']]
    df = inject_owid_aggregates(df)
    df = discard_rows(df)
    df = inject_total_daily_cols(df, ['cases', 'deaths'])
    df = inject_weekly_growth(df)
    df = inject_biweekly_growth(df)
    df = inject_doubling_days(df)
    df = inject_per_million(df, [
        'new_cases',
        'new_deaths',
        'total_cases',
        'total_deaths',
        'weekly_cases',
        'weekly_deaths',
        'biweekly_cases',
        'biweekly_deaths'
    ])
    df = inject_cfr(df)
    df = inject_rolling_avg(df)
    df = inject_days_since(df)
    df = inject_exemplars(df)
    return df.sort_values(by=['location', 'date'])

def export(filename):
    # locations.csv
    # load merged so that we exclude any past country names that exist in
    # ecdc_country_standardized but not in the current dataset
    df_loc = _load_merged(filename)[['countriesAndTerritories', 'location']] \
        .drop_duplicates()
    df_loc = df_loc.merge(
        load_owid_continents(),
        on='location',
        how='left'
    )
    df_loc = inject_population(df_loc)
    df_loc['population_year'] = df_loc['population_year'].round().astype('Int64')
    df_loc['population'] = df_loc['population'].round().astype('Int64')
    df_loc.to_csv(os.path.join(OUTPUT_PATH, 'locations.csv'), index=False)
    # The rest of the CSVs
    return standard_export(
        load_standardized(filename),
        OUTPUT_PATH,
        DATASET_NAME
    )

def run(filename=None, skip_download=False):
    import inquirer
    from glob import glob

    if not skip_download:
        print("\nAttempting to download latest report...")
        download_xlsx()
        download_csv()

    if filename is None:
        print(
    """\n[Note] If you don't see the latest report in the options below, please download the Excel file from:
    https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide
    Then move it to the folder %s\n""" % os.path.abspath(RELEASES_PATH))

        filenames = glob(os.path.join(RELEASES_PATH, '*.xlsx'))
        filenames.extend(glob(os.path.join(RELEASES_PATH, '*.xls')))
        filenames.extend(glob(os.path.join(RELEASES_PATH, '*.csv')))
        filenames = list(
            filter(
                lambda name: not name.startswith("~"),
                map(os.path.basename, sorted(filenames, reverse=True))
            )
        )

        answers = inquirer.prompt([
            inquirer.List('filename',
                    message='Which release to use?',
                    choices=filenames,
                    default=0)
        ])

        filename = answers['filename']

    if check_data_correctness(filename):
        print("Data correctness check %s.\n" % colored("passed", 'green'))
    else:
        print_err("Data correctness check %s.\n" % colored("failed", 'red'))
        sys.exit(1)

    if export(filename):
        print("Successfully exported CSVs to %s\n" % colored(os.path.abspath(OUTPUT_PATH), 'magenta'))
    else:
        print_err("ECDC Export failed.\n")
        sys.exit(1)

    print("Generating megafile…")
    megafile.generate_megafile()
    print("Megafile is ready.")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Run ECDC update script')
    parser.add_argument('filename', nargs='?', default=None, help="CSV/XLSX filename")
    parser.add_argument('-s', '--skip-download', action='store_true', help="Skip downloading files from the ECDC website")
    args = parser.parse_args()
    run(
        filename=args.filename,
        skip_download=args.skip_download
    )
