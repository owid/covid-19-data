import os
import sys
import pytz
from datetime import datetime, timedelta

CURRENT_DIR = os.path.dirname(__file__)
sys.path.append(CURRENT_DIR)

from utils.db_imports import import_dataset

DATASET_NAME = 'COVID testing time series data'
OUTPUT_PATH = os.path.join(CURRENT_DIR, '../grapher/')
OUTPUT_CSV_PATH = os.path.join(OUTPUT_PATH, f"{DATASET_NAME}.csv")
ZERO_DAY = "2020-01-21"

def update_db():
    time_str = (datetime.now() - timedelta(minutes=10)).astimezone(pytz.timezone('Europe/London')).strftime("%-d %B, %H:%M")
    source_name = f"Official data collated by Our World in Data – Last updated {time_str} (London time)"
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
