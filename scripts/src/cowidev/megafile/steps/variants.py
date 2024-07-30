import datetime

import pandas as pd

from cowidev.utils.s3 import obj_from_s3
from cowidev.utils.catalog import load_table_from_catalog


def get_variants(cases_file: str, variants_file: str) -> pd.DataFrame:
    """
    Fetches the processed data from CoVariants.org and merges it with biweekly cases from WHO/JHU.
    """
    tb = load_table_from_catalog(namespace="covid", dataset="sequence", table="variants")
    variants = read(variants_file, usecols=["location", "date", "num_sequences_total"]).drop_duplicates()
    cases = pd.read_csv(cases_file, usecols=["location", "date", "biweekly_cases"]).dropna()

    df = pd.merge(variants, cases, how="inner", on=["location", "date"], validate="one_to_one")

    df["share_cases_sequenced"] = df.num_sequences_total.mul(100).div(df.biweekly_cases).round(1)

    # Data quality: ensure that share is between 0 and 100, and add a 15 day cutoff to avoid showing
    # low sequencing rates due to lags in reporting.
    df = df[(df.share_cases_sequenced <= 100) & (df.share_cases_sequenced >= 0)]
    df = df[df.date <= str(datetime.date.today() - datetime.timedelta(days=15))]

    df = df.drop(columns=["num_sequences_total", "biweekly_cases"])
    return df


def read(path, **kwargs):
    if path.startswith("s3://"):
        return obj_from_s3(path, **kwargs)
    return pd.read_csv(path, **kwargs)
