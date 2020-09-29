import sys
import os
from glob import glob
import datetime

SKIP = []

execution_mode = sys.argv[1]
scripts_path = "automations/incremental/*.py" if execution_mode == "quick" else "automations/*/*.py"

if len(SKIP) > 0:
    print(f"Warning message:\nSkipping the following countries: {', '.join(SKIP)}")

for file in glob(scripts_path):
    print(f"{datetime.datetime.now().replace(microsecond=0)} - {file}")
    os.system(f"python3 {file}")
