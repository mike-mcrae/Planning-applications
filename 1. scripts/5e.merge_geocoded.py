import pandas as pd
import glob
import os

# =====================================================
# PATHS
# =====================================================

BASE_DIR = "/Users/mikemcrae/Documents/GitHub/Planning applications/0. data"

EXISTING_PATH = f"{BASE_DIR}/DCC_all_applications_geocoded.csv"
TEMP_PATTERN = f"{BASE_DIR}/temp_worker_*.csv"
FINAL_OUTPUT = EXISTING_PATH

# =====================================================
# LOAD EXISTING
# =====================================================

if os.path.exists(EXISTING_PATH):
    existing = pd.read_csv(EXISTING_PATH)
    print("Loaded existing:", len(existing))
else:
    existing = pd.DataFrame(columns=["application_number", "latitude", "longitude"])
    print("No existing file found.")

existing["application_number"] = existing["application_number"].astype(str)

# =====================================================
# LOAD WORKER FILES
# =====================================================

worker_files = glob.glob(TEMP_PATTERN)

print("Worker files found:", len(worker_files))

worker_dfs = []

for f in worker_files:
    try:
        df = pd.read_csv(f)
        print(f"Loaded {f} ({len(df)} rows)")
        worker_dfs.append(df)
    except Exception as e:
        print(f"Error reading {f}: {e}")

if worker_dfs:
    workers_combined = pd.concat(worker_dfs, ignore_index=True)
else:
    workers_combined = pd.DataFrame(columns=["application_number", "latitude", "longitude"])

workers_combined["application_number"] = workers_combined["application_number"].astype(str)

print("Total worker rows:", len(workers_combined))

# =====================================================
# COMBINE EXISTING + WORKERS
# =====================================================

combined = pd.concat([existing, workers_combined], ignore_index=True)

# Remove completely empty lat/lon rows
combined = combined.dropna(subset=["application_number"])

# Keep the last occurrence (so new worker data overrides old)
combined = combined.drop_duplicates(subset="application_number", keep="last")

print("Total unique applications:", len(combined))

# =====================================================
# SAVE FINAL
# =====================================================

combined.to_csv(FINAL_OUTPUT, index=False)

print("\nFinal merged file saved to:")
print(FINAL_OUTPUT)

# =====================================================
# OPTIONAL: CLEAN UP TEMP FILES
# =====================================================

delete_temp = input("\nDelete temp_worker files? (y/n): ")

if delete_temp.lower() == "y":
    for f in worker_files:
        os.remove(f)
    print("Temp files deleted.")
else:
    print("Temp files kept.")
