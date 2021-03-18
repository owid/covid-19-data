import os
import sys
import pytz
import json
import datetime
import time
from tqdm import tqdm
import pandas as pd


DATASET_NAME = 'YouGov-Imperial COVID-19 Behavior Tracker'

# MIN_RESPONSES: country-date-question observations with less than this
# many valid responses will be dropped. If "None", no observations will
# be dropped.
MIN_RESPONSES = 500

# FREQ: temporal level at which to aggregate the individual survey
# responses, passed as the `freq` argument to
# pandas.Series.dt.to_period. Must conform to a valid Pandas offset
# string (e.g. 'M' = "month", "W" = "week").
FREQ = 'M'

# ZERO_DAY: reference date for internal yearIsDay Grapher usage.
ZERO_DAY = "2020-01-21"

CURRENT_DIR = os.path.dirname(__file__)
sys.path.append(CURRENT_DIR)

INPUT_PATH = os.path.join(CURRENT_DIR, "../input/yougov")
OUTPUT_PATH = os.path.join(CURRENT_DIR, "../grapher")
OUTPUT_CSV_PATH = os.path.join(OUTPUT_PATH, f"{DATASET_NAME}.csv")

MAPPING = pd.read_csv(os.path.join(INPUT_PATH, "mapping.csv"), na_values=None)
MAPPING['label'] = MAPPING['label'].str.lower()
with open(os.path.join(INPUT_PATH, 'mapped_values.json'), 'r') as f:
    MAPPED_VALUES = json.load(f)


def update_csv():
    df = _merge_files()
    df = _subset_columns(df)
    df = _preprocess_cols(df)
    df = _standardize_entities(df)
    df = _aggregate(df)
    df = _rename_columns(df)
    df = _reorder_columns(df)
    df.to_csv(OUTPUT_CSV_PATH, index=False)


def update_db():
    from utils.db_imports import import_dataset

    time_str = datetime.datetime.now().astimezone(pytz.timezone('Europe/London')).strftime("%-d %B %Y, %H:%M")
    source_name = f"Imperial College London YouGov Covid 19 Behaviour Tracker Data Hub – Last updated {time_str} (London time)"
    import_dataset(
        dataset_name=DATASET_NAME,
        namespace='owid',
        csv_path=OUTPUT_CSV_PATH,
        default_variable_display={
            'yearIsDay': True,
            'zeroDay': ZERO_DAY
        },
        source_name=source_name,
        slack_notifications=False
    )


def _read_country_data(country, extension):
    return pd.read_csv(
        f"https://github.com/YouGov-Data/covid-19-tracker/raw/master/data/{country}.{extension}",
        low_memory=False,
        na_values=[
            "", "Not sure", " ", "Prefer not to say", "Don't know", 98, "Don't Know",
            "Not applicable - I have already contracted Coronavirus (COVID-19)",
            "Not applicable - I have already contracted Coronavirus"
        ]
    )


def _merge_files():

    all_data = []

    countries = list(pd.read_csv(
        "https://github.com/YouGov-Data/covid-19-tracker/raw/master/countries.csv", header=None
    )[0])

    for country in tqdm(countries):
        tqdm.write(country)
        try:
            df = _read_country_data(country, "csv")
        except:
            df = _read_country_data(country, "zip")
        try:
            df.loc[:, "date"] = pd.to_datetime(df.endtime, format="%d/%m/%Y %H:%M")
        except:
            df.loc[:, "date"] = pd.to_datetime(df.endtime, format="%Y-%m-%d %H:%M:%S")
        df.loc[:, "country"] = country
        df.columns = df.columns.str.lower()
        all_data.append(df)

    df = pd.concat(all_data, axis=0)

    assert df.columns.nunique() == df.columns.shape[0], 'There are one or more duplicate columns, which may cause unexpected errors.'
    
    return df


def _subset_columns(df):
    """keeps only the survey questions with keep=True in mapping.csv.
    """
    index_cols = ['country', 'date']
    assert MAPPING.keep.isin([True, False]).all(), 'All values in "keep" column of `MAPPING` must be True or False.'
    questions_keep = MAPPING.label[MAPPING.keep].tolist()
    df = df[index_cols + questions_keep]
    return df


def _preprocess_cols(df):
    for row in MAPPING[MAPPING.preprocess.notnull()].itertuples():
        if row.label in df.columns:
            df[row.label] = df[row.label].replace(MAPPED_VALUES[row.preprocess])
            uniq_values = set(MAPPED_VALUES[row.preprocess].values())
            assert df[row.label].drop_duplicates().dropna().isin(uniq_values).all(), f"One or more non-NaN values in {row.label} are not in {uniq_values}"
    return df


def _standardize_entities(df):
    df["entity"] = df.country.replace({
        "australia": "Australia",
        "brazil": "Brazil",
        "canada": "Canada",
        "china": "China",
        "denmark": "Denmark",
        "finland": "Finland",
        "france": "France",
        "germany": "Germany",
        "hong-kong": "Hong Kong",
        "india": "India",
        "indonesia": "Indonesia",
        "italy": "Italy",
        "japan": "Japan",
        "malaysia": "Malaysia",
        "mexico": "Mexico",
        "netherlands": "Netherlands",
        "norway": "Norway",
        "philippines": "Philippines",
        "saudi-arabia": "Saudi Arabia",
        "singapore": "Singapore",
        "south-korea": "South Korea",
        "spain": "Spain",
        "sweden": "Sweden",
        "taiwan": "Taiwan",
        "thailand": "Thailand",
        "united-arab-emirates": "United Arab Emirates",
        "united-kingdom": "United Kingdom",
        "united-states": "United States",
        "vietnam": "Vietnam"
    })
    df = df.drop(columns=["country"])
    return df


def _aggregate(df):
    s_period = df["date"].dt.to_period(FREQ)
    df.loc[:, "date_end"] = s_period.dt.end_time.dt.date
    today = datetime.datetime.utcnow().date()
    if df['date_end'].max() > today:
        df.loc[:, "date_end"] = df['date_end'].replace({df['date_end'].max(): today})
    
    questions = [q for q in MAPPING.label.tolist() if q in df.columns]

    # computes the mean for each country-date-question observation
    # (returned in long format)
    df_means = df.groupby(["entity", "date_end"])[questions] \
                 .mean() \
                 .round(1) \
                 .rename_axis('question', axis=1) \
                 .stack() \
                 .rename('mean') \
                 .to_frame()
    
    # counts the number of non-NaN responses for each country-date-question
    # observation (returned in long format)
    df_counts = df.groupby(["entity", "date_end"])[questions] \
                  .apply(lambda gp: gp.notnull().sum()) \
                  .rename_axis('question', axis=1) \
                  .stack() \
                  .rename('num_responses') \
                  .to_frame()
    
    df_agg = pd.merge(df_means, df_counts, left_index=True, right_index=True, how='outer', validate='1:1')
    
    if MIN_RESPONSES:
        df_agg = df_agg[df_agg['num_responses'] >= MIN_RESPONSES]
    
    # converts dataframe back to wide format.
    df_agg = df_agg.unstack().reset_index()
    df_agg.columns = [f'{lvl1}__{lvl0}' if lvl1 else lvl0 for lvl0, lvl1 in df_agg.columns]
    df_agg.rename(columns={'date_end': 'date'}, inplace=True)

    # constructs date variable for internal Grapher usage.
    df_agg.loc[:, "date_internal_use"] = (df_agg['date'] - datetime.datetime.strptime(ZERO_DAY, '%Y-%m-%d')).dt.days
    df_agg.drop('date', axis=1, inplace=True)

    return df_agg


def _rename_columns(df):
    suffixes = ['mean', 'num_responses']
    rename_dict = {}
    for row in MAPPING.itertuples():
        for sfx in suffixes:
            key = f'{row.label}__{sfx}'
            if key in df.columns:
                val = row.code_name if sfx == 'mean' else f'{row.code_name}__{sfx}'
                rename_dict[key] = val
    df = df.rename(columns=rename_dict)

    # renames index columns for use in `update_db`.
    df = df.rename(columns={'entity': 'Country', 'date_internal_use': 'Year'})
    return df


def _reorder_columns(df):
    index_cols = ['Country', 'Year']
    data_cols = sorted([col for col in df.columns if col not in index_cols])
    df = df[index_cols + data_cols]
    return df


if __name__ == "__main__":
    update_csv()
