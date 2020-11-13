import os
import datetime
import time
from glob import glob
from tqdm import tqdm
import pandas as pd


CURRENT_DIR = os.path.dirname(__file__)
INPUT_PATH = os.path.join(CURRENT_DIR, "../input/yougov")
OUTPUT_PATH = os.path.join(CURRENT_DIR, "../../public/data/yougov")
MAPPING = pd.read_csv(os.path.join(INPUT_PATH, "mapping.csv"), na_values=None)

MAPPED_VALUES = {
    "binary": {"No": 0, "Yes": 100},
    "frequency": {"Not at all": 0, "Rarely": 0, "Sometimes": 0, "Frequently": 100, "Always": 100},
    "easiness": {
        "Very easy": 0,
        "Somewhat easy": 0,
        "Neither easy nor difficult": 0,
        "Somewhat difficult": 100,
        "Very difficult": 100
    },
    "willingness": {
        "Very unwilling": 0,
        "Somewhat unwilling": 0,
        "Neither willing nor unwilling": 0,
        "Somewhat willing": 100,
        "Very willing": 100
    },
    "scariness": {
        "I am not at all scared that I will contract the Coronavirus (COVID-19)": 0,
        "I am not very scared that I will contract the Coronavirus (COVID-19)": 0,
        "I am fairly scared that I will contract the Coronavirus (COVID-19)": 100,
        "I am very scared that I will contract the Coronavirus (COVID-19)": 100
    },
    "happiness": {
        "Much less happy now": 0,
        "Somewhat less happy now": 0,
        "About the same": 0,
        "Somewhat more happy now": 100,
        "Much more happy now": 100
    },
    "handling": {"Very badly": 0, "Somewhat badly": 0, "Somewhat well": 100, "Very well": 100},
    "agreement": {"1 – Disagree": 0, "2": 0, "3": 0, "4": 0, "5": 100, "6": 100, "7 - Agree": 100},
    "trustworthiness": {
        "1 - Not at all trustworthy": 0,
        "2": 0,
        "3": 0,
        "4": 100,
        "5 - Completely trustworthy": 100
    },
    "efficiency": {
        "1 - Not efficient at all": 0,
        "2": 0,
        "3": 0,
        "4": 100,
        "5 - Extremely efficient": 100
    },
    "unity": {"More divided": 0, "No change": 0, "More united": 100},
    "strength": {"Very weak": 0, "Somewhat weak": 0, "Somewhat strong": 100, "Very strong": 100},
    "increase": {"Decreased": 0, "No change": 0, "Increased": 100}
}


def merge_files():

    all_data = []

    countries = list(pd.read_csv(
        "https://github.com/YouGov-Data/covid-19-tracker/raw/master/countries.csv", header=None
    )[0])

    # Data for the UK is split between two files in the repo
    countries.remove("united-kingdom")
    countries += ["united-kingdom1", "united-kingdom2"]

    for country in tqdm(countries):
        tqdm.write(country)
        df = pd.read_csv(
            f"https://github.com/YouGov-Data/covid-19-tracker/raw/master/data/{country}.csv",
            low_memory=False,
            na_values=[
                "", "Not sure", " ", "Prefer not to say", "Don't know", 98, "Don't Know",
                "Not applicable - I have already contracted Coronavirus (COVID-19)",
                "Not applicable - I have already contracted Coronavirus"
            ]
        )
        df.loc[:, "country"] = country
        all_data.append(df)

    df = pd.concat(all_data)
    return df


def make_weekly(df):
    df.loc[:, "Date"] = (
        pd.to_datetime(df.endtime, format="%d/%m/%Y %H:%S")
        .dt.to_period('W')
        .apply(lambda r: r.start_time)
    )
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
        "united-kingdom1": "United Kingdom",
        "united-kingdom2": "United Kingdom",
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
    for row in MAPPING.itertuples():
        code_name = row.code_name
        df = df.rename(columns={row.label: code_name})
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
