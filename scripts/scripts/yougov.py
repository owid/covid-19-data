import os
import json
import datetime
import time
from tqdm import tqdm
import pandas as pd


CURRENT_DIR = os.path.dirname(__file__)
INPUT_PATH = os.path.join(CURRENT_DIR, "../input/yougov")
OUTPUT_PATH = os.path.join(CURRENT_DIR, "../grapher")
MAPPING = pd.read_csv(os.path.join(INPUT_PATH, "mapping.csv"), na_values=None)
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
            df.loc[:, "Date"] = pd.to_datetime(df.endtime, format="%d/%m/%Y %H:%M")
        except:
            df.loc[:, "Date"] = pd.to_datetime(df.endtime, format="%Y-%m-%d %H:%M:%S")
        df.loc[:, "country"] = country
        all_data.append(df)

    df = pd.concat(all_data)
    return df


def make_weekly(df):
    df.loc[:, "Date"] = df["Date"].dt.to_period("W").apply(lambda r: r.start_time)
    df.loc[:, "Date"] = (df.Date - datetime.datetime(2020, 1, 21)).dt.days
    df = df.drop(columns=["endtime", "RecordNo"])
    return df


def remove_small_samples(df):
    samples = df.groupby(["country", "Date"], as_index=False).size()
    low_samples = samples[samples["size"] < 30]
    low_samples = zip(low_samples.country, low_samples.Date)
    df = df[-pd.Series(list(zip(df.country, df.Date)), index=df.index).isin(low_samples)]
    return df


def preprocess_cols(df):
    for row in MAPPING[-MAPPING.preprocess.isna()].itertuples():
        df[row.label] = df[row.label].replace(MAPPED_VALUES[row.preprocess])
    return df


def standardize_entities(df):
    df["Entity"] = df.country.replace({
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
    df = df.groupby(["Entity", "Date"], as_index=False).mean().round(1)
    return df


def rename_columns(df):
    df = df[["Entity", "Date"] + list(MAPPING.label)]
    df = df.rename(columns=dict(zip(MAPPING.label, MAPPING.code_name)))
    return df


def main():
    df = merge_files()
    df = make_weekly(df)
    df = remove_small_samples(df)
    df = preprocess_cols(df)
    df = standardize_entities(df)
    df = aggregate(df)
    df = rename_columns(df)
    df.to_csv(
        os.path.join(OUTPUT_PATH, "YouGov-Imperial COVID-19 Behavior Tracker.csv"), index=False
    )
    

if __name__ == "__main__":
    main()
