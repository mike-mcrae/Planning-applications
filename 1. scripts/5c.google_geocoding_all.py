import pandas as pd
import requests
import os
import time
from datetime import datetime

# =====================================================
# CONFIG
# =====================================================

API_KEY = os.environ.get("GOOGLE_GEOCODING_API_KEY")

APPS_PATH = "/Users/mikemcrae/Documents/GitHub/Planning applications/0. data/IrishPlanningApplications_DublinCityCouncil.csv"
OBS_PATH = "/Users/mikemcrae/Documents/GitHub/Planning applications/1. scripts/outputs/third_party_obs_merged.csv"

EXISTING_GEO_PATH = "/Users/mikemcrae/Documents/GitHub/Planning applications/0. data/DCC_all_applications_geocoded.csv"
OUTPUT_PATH = EXISTING_GEO_PATH

REQUESTS_PER_MIN = 1400
DELAY = 60 / REQUESTS_PER_MIN   # ~0.043 sec

SAVE_INTERVAL = 200

# =====================================================
# LOAD DATA
# =====================================================

apps = pd.read_csv(APPS_PATH)
obs = pd.read_csv(OBS_PATH)

obs["n_observation_letters"] = pd.to_numeric(
    obs["n_observation_letters"], errors="coerce"
).fillna(0)

apps["Application Number"] = apps["Application Number"].astype(str).str.strip()
obs["application_number"] = obs["application_number"].astype(str).str.strip()

# =====================================================
# MERGE ALL APPLICATIONS
# =====================================================

df_all = obs.merge(
    apps,
    left_on="application_number",
    right_on="Application Number",
    how="inner"
)

df_all["full_address"] = (
    df_all["Development Address"].astype(str) + ", Dublin, Ireland"
)

# =====================================================
# LOAD EXISTING (IF ANY)
# =====================================================

if os.path.exists(EXISTING_GEO_PATH):
    existing = pd.read_csv(EXISTING_GEO_PATH)
    existing["application_number"] = existing["application_number"].astype(str)
    print("Resuming from existing file.")
else:
    existing = pd.DataFrame(columns=["application_number", "latitude", "longitude"])
    print("No existing file found. Starting fresh.")

already_done = set(existing["application_number"])

print("Already geocoded:", len(already_done))

df_to_geocode = df_all[
    ~df_all["application_number"].isin(already_done)
].copy()

print("Remaining to geocode:", len(df_to_geocode))

# =====================================================
# GOOGLE GEOCODING FUNCTION
# =====================================================

def geocode_address(address):
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": address,
        "key": API_KEY
    }

    response = requests.get(url, params=params)
    data = response.json()

    if data["status"] == "OK":
        loc = data["results"][0]["geometry"]["location"]
        return loc["lat"], loc["lng"]

    elif data["status"] == "OVER_QUERY_LIMIT":
        print("Rate limit hit — sleeping 10 seconds...")
        time.sleep(10)
        return geocode_address(address)

    else:
        return None, None

# =====================================================
# GEOCODING LOOP (RESUMABLE + PACED)
# =====================================================

start_time = time.time()
new_rows = []

for idx, row in df_to_geocode.iterrows():

    lat, lon = geocode_address(row["full_address"])

    new_rows.append({
        "application_number": row["application_number"],
        "latitude": lat,
        "longitude": lon
    })

    # pacing
    time.sleep(DELAY)

    # progress reporting
    processed = len(new_rows)
    remaining = len(df_to_geocode) - processed

    if processed % 100 == 0:
        elapsed = time.time() - start_time
        rate = processed / elapsed
        eta = remaining / rate if rate > 0 else 0

        print(
            f"Processed: {processed} | "
            f"Remaining: {remaining} | "
            f"ETA: {int(eta/60)} min"
        )

    # periodic save
    if processed % SAVE_INTERVAL == 0:
        temp_df = pd.DataFrame(new_rows)
        combined = pd.concat([existing, temp_df], ignore_index=True)
        combined.to_csv(OUTPUT_PATH, index=False)
        print("Progress saved.")

# =====================================================
# FINAL SAVE
# =====================================================

temp_df = pd.DataFrame(new_rows)
combined = pd.concat([existing, temp_df], ignore_index=True)
combined = combined.drop_duplicates(subset="application_number")

combined.to_csv(OUTPUT_PATH, index=False)

total_time = (time.time() - start_time) / 60

print("\nGeocoding complete.")
print(f"Total time: {round(total_time,2)} minutes")
print("Total applications:", len(combined))
