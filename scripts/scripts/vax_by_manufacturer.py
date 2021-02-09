import os
import sys
import pytz
from datetime import datetime, timedelta

CURRENT_DIR = os.path.dirname(__file__)
sys.path.append(CURRENT_DIR)

from utils.db_imports import import_dataset

DATASET_NAME = 'COVID-19 - Vaccinations by manufacturer'
OUTPUT_PATH = os.path.join(CURRENT_DIR, '../grapher/')
OUTPUT_CSV_PATH = os.path.join(OUTPUT_PATH, f"{DATASET_NAME}.csv")
ZERO_DAY = "2021-01-01"

def update_db():
    import_dataset(
        dataset_name=DATASET_NAME,
        namespace='owid',
        csv_path=OUTPUT_CSV_PATH,
        default_variable_display={
            'yearIsDay': True,
            'zeroDay': ZERO_DAY
        },
        source_name="Official data collated by Our World in Data",
        slack_notifications=False
    )
