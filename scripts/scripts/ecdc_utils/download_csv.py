import sys
import os

CURRENT_DIR = os.path.dirname(__file__)

sys.path.append(os.path.join(CURRENT_DIR, ".."))

import ecdc

if __name__ == "__main__":
    ecdc.download_csv()
