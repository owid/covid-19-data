import os
import sys
import pandas as pd
import pytz
from datetime import datetime

CURRENT_DIR = os.path.dirname(__file__)
sys.path.append(CURRENT_DIR)

from utils.db_imports import import_dataset

URL = "https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_latest.csv"
DATASET_NAME = 'COVID Government Response (OxBSG)'

INPUT_PATH = os.path.join(CURRENT_DIR, "../input/bsg/")
OUTPUT_PATH = os.path.join(CURRENT_DIR, '../grapher/')
INPUT_CSV_PATH = os.path.join(INPUT_PATH, 'latest.csv')
OUTPUT_CSV_PATH = os.path.join(OUTPUT_PATH, f"{DATASET_NAME}.csv")

ZERO_DAY = "2020-01-01"
zero_day = datetime.strptime(ZERO_DAY, "%Y-%m-%d")

def download_csv():
    os.system('curl --silent -f -o %(INPUT_CSV_PATH)s -L %(URL)s' % {
        'INPUT_CSV_PATH': INPUT_CSV_PATH,
        'URL': URL
    })

def export_grapher():

    cols = [
        "CountryName",
        "Date",
        "C1_School closing",
        "C2_Workplace closing",
        "C3_Cancel public events",
        "C4_Restrictions on gatherings",
        "C5_Close public transport",
        "C6_Stay at home requirements",
        "C7_Restrictions on internal movement",
        "C8_International travel controls",
        "E1_Income support",
        "E2_Debt/contract relief",
        "E3_Fiscal measures",
        "E4_International support",
        "H1_Public information campaigns",
        "H2_Testing policy",
        "H3_Contact tracing",
        "H4_Emergency investment in healthcare",
        "H5_Investment in vaccines",
        "H6_Facial Coverings",
        "H7_Vaccination policy",
        "StringencyIndex",
        "ContainmentHealthIndex"
    ]

    cgrt = pd.read_csv(INPUT_CSV_PATH, low_memory=False)

    if "RegionCode" in cgrt.columns:
        cgrt = cgrt[cgrt.RegionCode.isnull()]

    cgrt = cgrt[cols]

    cgrt.loc[:, "Date"] = pd.to_datetime(cgrt["Date"], format="%Y%m%d").map(
        lambda date: (date - zero_day).days
    )

    rows_before = cgrt.shape[0]

    country_mapping = pd.read_csv(os.path.join(INPUT_PATH, "bsg_country_standardised.csv"))

    cgrt = country_mapping.merge(cgrt, on="CountryName", how="right")

    missing_from_mapping = cgrt[cgrt["Country"].isna()]["CountryName"].unique()
    if len(missing_from_mapping) > 0:
        raise Exception(f"Missing countries in mapping: {missing_from_mapping}")

    cgrt = cgrt.drop(columns=["CountryName"])

    rename_dict = {
        "Date": "Year",
        "C1_School closing": "school_closures",
        "C2_Workplace closing": "workplace_closures",
        "C3_Cancel public events": "cancel_public_events",
        "C5_Close public transport": "close_public_transport",
        "H1_Public information campaigns": "public_information_campaigns",
        "C7_Restrictions on internal movement": "restrictions_internal_movements",
        "C8_International travel controls": "international_travel_controls",
        "E3_Fiscal measures": "fiscal_measures",
        "H4_Emergency investment in healthcare": "emergency_investment_healthcare",
        "H5_Investment in vaccines": "investment_vaccines",
        "H3_Contact tracing": "contact_tracing",
        "H6_Facial Coverings": "facial_coverings",
        "StringencyIndex": "stringency_index",
        "ContainmentHealthIndex": "containment_index",
        "C4_Restrictions on gatherings": "restriction_gatherings",
        "C6_Stay at home requirements": "stay_home_requirements",
        "E1_Income support": "income_support",
        "E2_Debt/contract relief": "debt_relief",
        "E4_International support": "international_support",
        "H7_Vaccination policy": "vaccination_policy",
        "H2_Testing policy": "testing_policy"
    }

    cgrt = cgrt.rename(columns=rename_dict).sort_values(["Country", "Year"])

    os.system('mkdir -p %s' % os.path.abspath(OUTPUT_PATH))
    cgrt.to_csv(OUTPUT_CSV_PATH, index=False)

def update_db():
    time_str = datetime.now().astimezone(pytz.timezone('Europe/London')).strftime("%-d %B, %H:%M")
    source_name = f"Hale, Angrist, Goldszmidt, Kira, Petherick, Phillips, Webster, Cameron-Blake, Hallas, Majumdar, and Tatlow (2021). “A global panel database of pandemic policies (Oxford COVID-19 Government Response Tracker).” Nature Human Behaviour. – Last updated {time_str} (London time)"
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

if __name__ == '__main__':
    download_csv()
    export_grapher()
