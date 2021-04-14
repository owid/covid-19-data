"""
from joblib import Parallel, delayed
def process(i):
    return i * i
    
results = Parallel(n_jobs=2)(delayed(process)(i) for i in range(10))
print(results)
"""
import logging
import importlib
from datetime import datetime

from vax.batch import __all__ as batch_countries
from vax.incremental import __all__ as incremental_countries


logger = logging.Logger('catch_all')
SKIP_COUNTRIES = ["canada"]

batch_countries = [f"vax.batch.{c}" for c in batch_countries]
incremental_countries = [f"vax.incremental.{c}" for c in incremental_countries]
modules_name = batch_countries + incremental_countries


def main_get_data():
    """Get data from sources and export to output folder.
    
    Is equivalent to script `run_python_scripts.py`
    """
    modules_name = ["vax.incremental.canada", "vax.incremental.netherlands", "vax.incremental.morocco"]
    modules_failed = []
    for module_name in modules_name:
        date_str = datetime.now().strftime("%Y-%m-%d %X")
        print(f">> {date_str} - {module_name}")
        country = module_name.split(".")[-1]
        if country in SKIP_COUNTRIES:
            print("    skipped!")
            continue
        module = importlib.import_module(module_name)
        try:
            module.main()
        except Exception as err:
            logger.error(err, exc_info=True)
            modules_failed.append(module_name)
            print()

    # Retry failed modules
    print(f"\n---\n\nRETRIALS ({len(modules_failed)})")
    for module_name in modules_failed:
        date_str = datetime.now().strftime("%Y-%m-%d %X")
        print(f">> {date_str} - {module_name} - (RETRIAL)")
        module = importlib.import_module(module_name)
        try:
            module.main()
        except Exception as err:
            logger.error(err, exc_info=True)
            print()

    if len(modules_failed) > 0:
        print(f"\n---\n\nThe following scripts failed to run ({len(modules_failed)}):")
        print("\n".join([f"* {m}" for m in modules_failed]))


if __name__ == "__main__":
    main_get_data()
