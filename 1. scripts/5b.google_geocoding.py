import pandas as pd
import requests
import os
import time

# =========================
# CONFIG
# =========================

API_KEY = os.environ.get("GOOGLE_GEOCODING_API_KEY")

INPUT_PATH = "/Users/mikemcrae/Documents/GitHub/Planning applications/0. data/IrishPlanningApplications_DublinCityCouncil.csv"
OBS_PATH = "/Users/mikemcrae/Documents/GitHub/Planning applications/1. scripts/outputs/third_party_obs_merged.csv"

OUTPUT_PATH = "/Users/mikemcrae/Documents/GitHub/Planning applications/DCC_objections_geocoded.csv"

# =========================
# LOAD DATA
# =========================

apps = pd.read_csv(INPUT_PATH)
obs = pd.read_csv(OBS_PATH)

obs["n_observation_letters"] = pd.to_numeric(
    obs["n_observation_letters"], errors="coerce"
).fillna(0)

# Keep only objection cases
obs = obs[obs["n_observation_letters"] > 0].copy()

# Merge
apps["Application Number"] = apps["Application Number"].astype(str).str.strip()
obs["application_number"] = obs["application_number"].astype(str).str.strip()

df = obs.merge(
    apps,
    left_on="application_number",
    right_on="Application Number",
    how="inner"
)

print("Applications with objections:", len(df))

# Create full address
df["full_address"] = (
    df["Development Address"].astype(str) + ", Dublin, Ireland"
)

# Add lat/long columns
if "latitude" not in df.columns:
    df["latitude"] = None
    df["longitude"] = None

# =========================
# GOOGLE GEOCODING FUNCTION
# =========================

def geocode_address(address):
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": address,
        "key": API_KEY
    }
    response = requests.get(url, params=params)
    data = response.json()

    if data["status"] == "OK":
        location = data["results"][0]["geometry"]["location"]
        return location["lat"], location["lng"]

    elif data["status"] == "OVER_QUERY_LIMIT":
        print("Hit query limit — sleeping 5 seconds...")
        time.sleep(5)
        return geocode_address(address)

    else:
        return None, None

# =========================
# GEOCODE LOOP
# =========================

for i, row in df.iterrows():

    if pd.notnull(row["latitude"]):
        continue

    lat, lon = geocode_address(row["full_address"])

    df.at[i, "latitude"] = lat
    df.at[i, "longitude"] = lon

    if i % 50 == 0:
        print(f"Processed {i}/{len(df)}")
        df.to_csv(OUTPUT_PATH, index=False)

    time.sleep(0.1)  # polite throttling

df.to_csv(OUTPUT_PATH, index=False)

print("\nGeocoding complete.")
