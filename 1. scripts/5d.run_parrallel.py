import pandas as pd
import requests
import os
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

API_KEY = os.environ.get("GOOGLE_GEOCODING_API_KEY")

APPS_PATH = "/Users/mikemcrae/Documents/GitHub/Planning applications/0. data/IrishPlanningApplications_DublinCityCouncil.csv"
OBS_PATH = "/Users/mikemcrae/Documents/GitHub/Planning applications/1. scripts/outputs/third_party_obs_merged.csv"
EXISTING_PATH = "/Users/mikemcrae/Documents/GitHub/Planning applications/0. data/DCC_all_applications_geocoded.csv"

OUTPUT_TEMPLATE = "/Users/mikemcrae/Documents/GitHub/Planning applications/0. data/temp_worker_{worker}.csv"

# Worker arguments
WORKER_ID = int(sys.argv[1])
N_WORKERS = int(sys.argv[2])

# Safe per-worker rate
REQUESTS_PER_SECOND = 2
DELAY = 1 / REQUESTS_PER_SECOND

# =====================================================
# LOAD DATA
# =====================================================

apps = pd.read_csv(APPS_PATH)
obs = pd.read_csv(OBS_PATH)

apps["Application Number"] = apps["Application Number"].astype(str).str.strip()
obs["application_number"] = obs["application_number"].astype(str).str.strip()

df = obs.merge(
    apps,
    left_on="application_number",
    right_on="Application Number",
    how="inner"
)

df["full_address"] = df["Development Address"].astype(str) + ", Dublin, Ireland"

# Remove already completed
if os.path.exists(EXISTING_PATH):
    existing = pd.read_csv(EXISTING_PATH)
    done = set(existing["application_number"].astype(str))
else:
    done = set()

df = df[~df["application_number"].isin(done)]

# Split work by modulo
df = df.iloc[WORKER_ID::N_WORKERS].copy()

print(f"Worker {WORKER_ID} processing {len(df)} rows.")

def geocode(address):
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": address, "key": API_KEY}

    response = requests.get(url, params=params)
    data = response.json()

    if data["status"] == "OK":
        loc = data["results"][0]["geometry"]["location"]
        return loc["lat"], loc["lng"]
    else:
        return None, None

results = []

for i, row in df.iterrows():

    lat, lon = geocode(row["full_address"])

    results.append({
        "application_number": row["application_number"],
        "latitude": lat,
        "longitude": lon
    })

    time.sleep(DELAY)

    if len(results) % 200 == 0:
        pd.DataFrame(results).to_csv(
            OUTPUT_TEMPLATE.format(worker=WORKER_ID),
            index=False
        )
        print(f"Worker {WORKER_ID}: saved {len(results)}")

# Final save
pd.DataFrame(results).to_csv(
    OUTPUT_TEMPLATE.format(worker=WORKER_ID),
    index=False
)

print(f"Worker {WORKER_ID} finished.")
