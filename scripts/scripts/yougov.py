import os
import json
import datetime
import time
from tqdm import tqdm
import pandas as pd


# MIN_RESPONSES: country-date-question observations with less than
# this many valid responses will be dropped. If "None", no observations will
# be dropped.
MIN_RESPONSES = 500

# FREQ: temporal level at which to aggregate the individual survey
# responses, passed as the `freq` argument to pandas.Series.dt.to_period. Must
# conform to a valid Pandas offset string (e.g. 'M' = "month", "W" =
# "week").
FREQ = 'M'

# INTERNAL_REF_DATE: reference date for internal Grapher usage.
INTERNAL_REF_DATE = datetime.datetime(2020, 1, 21)


CURRENT_DIR = os.path.dirname(__file__)
INPUT_PATH = os.path.join(CURRENT_DIR, "../input/yougov")
OUTPUT_PATH = os.path.join(CURRENT_DIR, "../grapher")
OUTPUT_FNAME = "YouGov-Imperial COVID-19 Behavior Tracker.csv"

MAPPING = pd.read_csv(os.path.join(INPUT_PATH, "mapping.csv"), na_values=None)
MAPPING['label'] = MAPPING['label'].str.lower()
with open(os.path.join(INPUT_PATH, 'mapped_values.json'), 'r') as f:
    MAPPED_VALUES = json.load(f)


def read_country_data(country, extension):
    df = pd.read_csv(
        f"https://github.com/YouGov-Data/covid-19-tracker/raw/master/data/{country}.{extension}",
        low_memory=False,
        na_values=[
            "", "Not sure", " ", "Prefer not to say", "Don't know", 98, "Don't Know",
            "Not applicable - I have already contracted Coronavirus (COVID-19)",
            "Not applicable - I have already contracted Coronavirus"
        ]
    )
    return df


def merge_files():

    all_data = []

    countries = list(pd.read_csv(
        "https://github.com/YouGov-Data/covid-19-tracker/raw/master/countries.csv", header=None
    )[0])

    for country in tqdm(countries):
        tqdm.write(country)
        try:
            df = read_country_data(country, "csv")
        except:
            df = read_country_data(country, "zip")
        try:
            df.loc[:, "date"] = pd.to_datetime(df.endtime, format="%d/%m/%Y %H:%M")
        except:
            df.loc[:, "date"] = pd.to_datetime(df.endtime, format="%Y-%m-%d %H:%M:%S")
        df.loc[:, "country"] = country
        all_data.append(df)

    df = pd.concat(all_data)

    df.columns = df.columns.str.lower()
    assert df.columns.nunique() == df.columns.shape[0], 'There are one or more duplicate columns, which may cause unexpected errors.'
    
    return df


def preprocess_cols(df):
    for row in MAPPING[-MAPPING.preprocess.isna()].itertuples():
        df[row.label] = df[row.label].replace(MAPPED_VALUES[row.preprocess])
        uniq_values = set(MAPPED_VALUES[row.preprocess].values())
        assert df[row.label].drop_duplicates().dropna().isin(uniq_values).all(), f"One or more non-NaN values in {row.label} are not in {uniq_values}"
    return df


def standardize_entities(df):
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


def aggregate(df):
    s_period = df["date"].dt.to_period(FREQ)
    df.loc[:, "date_end"] = s_period.dt.end_time.dt.date
    questions = MAPPING.label.tolist()

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
    df_agg.loc[:, "date_internal_use"] = (df_agg['date'] - INTERNAL_REF_DATE).dt.days

    return df_agg


def rename_columns(df):
    suffixes = ['mean', 'num_responses']
    df = df.rename(
        columns={f'{row.label}__{sfx}': f'{row.code_name}__{sfx}' for row in MAPPING.itertuples() for sfx in suffixes}
    )
    return df


def reorder_columns(df):
    index_cols = ['entity', 'date', 'date_internal_use']
    data_cols = sorted([col for col in df.columns if col not in index_cols])
    df = df[index_cols + data_cols]
    return df

def main():
    df = merge_files()
    df = preprocess_cols(df)
    df = standardize_entities(df)
    df = aggregate(df)
    df = rename_columns(df)
    df = reorder_columns(df)
    df.to_csv(os.path.join(OUTPUT_PATH, OUTPUT_FNAME), index=False)


if __name__ == "__main__":
    main()
