import os
import sys
import pandas as pd

CURRENT_DIR = os.path.dirname(__file__)

sys.path.append(CURRENT_DIR)

from shared import load_population, inject_total_daily_cols, inject_world, inject_per_million, inject_days_since_all, inject_population, standard_export

INPUT_PATH = os.path.join(CURRENT_DIR, '../input/ecdc/')
OUTPUT_PATH = os.path.join(CURRENT_DIR, '../public/data/ecdc/')
TMP_PATH = os.path.join(CURRENT_DIR, '../tmp')

LOCATIONS_CSV_PATH = os.path.join(INPUT_PATH, 'locations.csv')
DATA_XLS_PATH = os.path.join(INPUT_PATH, 'releases', '2020-03-19.xlsx')

def load_data():
    return pd.read_excel(
        DATA_XLS_PATH,
        # Namibia has 'NA' 2-letter code, we don't want that to be <NA>
        keep_default_na=False
    )

def load_locations():
    return pd.read_csv(
        LOCATIONS_CSV_PATH,
        keep_default_na=False,
        index_col=['GeoId', 'Countries and territories']
    )

def _load_merged():
    df_data = load_data()
    df_locs = load_locations()
    return df_data.merge(
        df_locs,
        how='left',
        on=['GeoId', 'Countries and territories'],
    )

def check_data_correctness():
    errors = 0
    df_merged = _load_merged()
    df_uniq = df_merged[['Countries and territories', 'GeoId', 'location']].drop_duplicates()
    df_pop = load_population()
    if df_uniq['location'].isnull().any():
        print("Error: Could not find OWID names for:")
        print(df_uniq[df_uniq['location'].isnull()])
        csv_path = os.path.join(TMP_PATH, 'ecdc_locations.csv')
        print(os.path.abspath(TMP_PATH))
        os.system('mkdir -p %s' % os.path.abspath(TMP_PATH))
        df_uniq \
            .sort_values(by=['location']) \
            .to_csv(csv_path, index=False)
        print("Saved CSV file to be standardized at %s" % csv_path)
        errors += 1
    if df_merged.duplicated(subset=['DateRep', 'location']).any():
        print("Found duplicate rows:")
        print(df_merged[df_merged.duplicated(subset=['DateRep', 'Our World in Data'])])
        errors += 1
    pop_entity_diff = set(df_uniq['location']) - set(df_pop['location'])
    if len(pop_entity_diff) > 0:
        # this is not an error, so don't increment errors variable
        print("These entities were not found in the population dataset:")
        print(pop_entity_diff)
    return True if errors == 0 else False

# Must output columns:
# date, location, new_cases, new_deaths, total_cases, total_deaths
def load_standardized():
    df = _load_merged() \
        .drop(columns=[
            'Countries and territories', 'GeoId',
            'Day', 'Month', 'Year',
        ]) \
        .rename(columns={
            'DateRep': 'date',
            'Cases': 'new_cases',
            'Deaths': 'new_deaths'
        })
    df = df[['date', 'location', 'new_cases', 'new_deaths']]
    df = inject_world(df)
    df = inject_total_daily_cols(df, ['cases', 'deaths'])
    df = inject_per_million(df, [
        'new_cases',
        'new_deaths',
        'total_cases',
        'total_deaths'
    ])
    df = inject_days_since_all(df)
    df_dupe_mask = df.duplicated(subset=['location', 'date'])
    if df_dupe_mask.any():
        print("Dataet contains duplicates for date, location:")
        df_dupes = df[df_dupe_mask]
        print(df_dupes)
        assert False
    return df.sort_values(by=['location', 'date'])

def export():
    # locations.csv
    df_loc = inject_population(load_locations().reset_index())
    df_loc['population_year'] = df_loc['population_year'].round().astype('Int64')
    df_loc['population'] = df_loc['population'].round().astype('Int64')
    df_loc.to_csv(os.path.join(OUTPUT_PATH, 'locations.csv'), index=False)
    # The rest of the CSVs
    return standard_export(load_standardized(), OUTPUT_PATH)

if __name__ == '__main__':
    if export():
        print("Successfully exported CSVs for ECDC.")
    else:
        print("ECDC Export failed.")
