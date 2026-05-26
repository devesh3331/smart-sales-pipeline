import pandas as pd
import os
import shutil
from datetime import datetime

# Simulate moving raw files into a "landing zone"
RAW_PATH = "data/raw/"
LANDING_PATH = "data/landing/"

os.makedirs(LANDING_PATH, exist_ok=True)

files = [f for f in os.listdir(RAW_PATH) if f.endswith('.csv')]

for file in files:
    src = os.path.join(RAW_PATH, file)
    dst = os.path.join(LANDING_PATH, file)
    shutil.copy(src, dst)
    print(f"[{datetime.now()}] Ingested: {file}")

print(f"\nTotal files ingested: {len(files)}")