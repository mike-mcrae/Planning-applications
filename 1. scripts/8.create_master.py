import pandas as pd

# =====================================================
# FILE PATHS
# =====================================================

ALL_APPS_PATH = "/Users/mikemcrae/Documents/GitHub/Planning applications/0. data/DCC_all_applications_geocoded.csv"
OBS_MERGED_PATH = "/Users/mikemcrae/Documents/GitHub/Planning applications/1. scripts/outputs/third_party_obs_merged_v2.csv"
OUTPUT_PATH = "/Users/mikemcrae/Documents/GitHub/Planning applications/1. scripts/outputs/applications_master_with_obs.csv"

# =====================================================
# LOAD DATA
# =====================================================

print("Loading all applications...")
df_apps = pd.read_csv(ALL_APPS_PATH)

print("Loading merged observation file...")
df_obs = pd.read_csv(OBS_MERGED_PATH)

print("Applications:", len(df_apps))
print("Observation file rows:", len(df_obs))

# =====================================================
# KEEP ONLY REQUIRED OBS COLUMNS
# =====================================================

# We only need:
# application_number + n_observation_letters

if "application_number" not in df_obs.columns:
    raise ValueError("application_number not found in observation file.")

if "n_observation_letters" not in df_obs.columns:
    raise ValueError("n_observation_letters not found in observation file.")

df_obs_clean = (
    df_obs[["application_number", "n_observation_letters"]]
    .groupby("application_number", as_index=False)
    .sum()
)

print("Unique applications in observation file:", len(df_obs_clean))

# =====================================================
# MERGE INTO FULL APPLICATION FILE
# =====================================================

df_master = df_apps.merge(
    df_obs_clean,
    on="application_number",
    how="left"
)

# Replace missing observation counts with 0
df_master["n_observation_letters"] = df_master["n_observation_letters"].fillna(0)

# Create binary indicator
df_master["has_observation"] = (df_master["n_observation_letters"] > 0).astype(int)

print("Final master rows:", len(df_master))
print("Applications with observations:", df_master["has_observation"].sum())

# =====================================================
# SAVE
# =====================================================

df_master.to_csv(OUTPUT_PATH, index=False)

print("Saved master file to:")
print(OUTPUT_PATH)
