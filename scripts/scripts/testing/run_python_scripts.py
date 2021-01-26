import sys
import os
import re
from glob import glob
import datetime

SKIP = []

execution_mode = sys.argv[1]
scripts_path = "automations/incremental/*.py" if execution_mode == "quick" else "automations/*/*.py"

scripts = glob(scripts_path)

if SKIP:
    print(f"Warning message:\nSkipping the following countries: {', '.join(SKIP)}")
    SKIP = "|".join(SKIP)
    scripts = [s for s in scripts if not bool(re.search(pattern=SKIP, string=s))]

for script_name in scripts:
    print(f"{datetime.datetime.now().replace(microsecond=0)} - {script_name}")
    os.system(f"python3 {script_name}")
