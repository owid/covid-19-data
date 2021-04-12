import os
from glob import glob
import datetime

scripts = glob("automations/*/*.py")

failed = []
failed_twice = []

for script_name in scripts:
    if "vaxutils.py" in script_name or "/archived/" in script_name:
        continue
    print(f"{datetime.datetime.now().replace(microsecond=0)} - {script_name}")
    result = os.system(f"python3 {script_name}")
    if result != 0:
        failed.append(script_name)

if len(failed) > 0:
    print("\n---\n\nRetrying:")
    
    for script_name in failed:
        print(f"{datetime.datetime.now().replace(microsecond=0)} - {script_name}")
        result = os.system(f"python3 {script_name}")
        if result != 0:
            failed_twice.append(script_name)

if len(failed_twice) > 0:
    print("\n---\n\nThe following scripts failed to run:")
    print("\n".join(failed_twice))

# Prepare files for generate_dataset.R
os.system(f"python3 collect_vax_data.py")
