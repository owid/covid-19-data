import os
from glob import glob
import datetime

scripts = glob("automations/*/*.py")

failed = []

for script_name in scripts:
    if "vaxutils.py" in script_name or "/archived/" in script_name:
        continue
    print(f"{datetime.datetime.now().replace(microsecond=0)} - {script_name}")
    result = os.system(f"python3 {script_name}")
    if result != 0:
        failed.append(script_name)

if len(failed) > 0:
    print("\n---\n\nThe following scripts failed to run:")
    print("\n".join(failed))
