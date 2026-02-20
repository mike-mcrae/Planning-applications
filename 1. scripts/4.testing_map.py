import pandas as pd
from pyproj import Transformer
import folium

OBS_PATH = "/Users/mikemcrae/Documents/GitHub/Planning applications/1. scripts/outputs/third_party_obs_merged.csv"
APPS_PATH = "/Users/mikemcrae/Documents/GitHub/Planning applications/0. data/IrishPlanningApplications_2359694955245257726.csv"

# -------------------------
# LOAD DATA
# -------------------------

obs = pd.read_csv(OBS_PATH)
apps = pd.read_csv(APPS_PATH, sep="\t", low_memory=False)

# Clean merge keys
obs["application_number"] = obs["application_number"].astype(str).str.strip()
apps["Application Number"] = apps["Application Number"].astype(str).str.strip()

# Force numeric objections
obs["n_observation_letters"] = pd.to_numeric(
    obs["n_observation_letters"],
    errors="coerce"
).fillna(0)

# Filter to Dublin City Council
apps_dcc = apps[apps["Planning Authority"] == "Dublin City Council"].copy()

# Convert ITM to numeric
apps_dcc["ITM Easting"] = pd.to_numeric(apps_dcc["ITM Easting"], errors="coerce")
apps_dcc["ITM Northing"] = pd.to_numeric(apps_dcc["ITM Northing"], errors="coerce")

print("Valid ITM Easting:", apps_dcc["ITM Easting"].notna().sum())
print("Valid ITM Northing:", apps_dcc["ITM Northing"].notna().sum())

# -------------------------
# MERGE
# -------------------------

df = obs.merge(
    apps_dcc,
    left_on="application_number",
    right_on="Application Number",
    how="inner"
)

print("Total Dublin City applications merged:", len(df))
print("Applications with objections:",
      (df["n_observation_letters"] > 0).sum())

# -------------------------
# CONVERT ITM → LAT/LONG
# -------------------------

transformer = Transformer.from_crs("EPSG:2157", "EPSG:4326", always_xy=True)

def convert_coords(row):
    if pd.notnull(row["ITM Easting"]) and pd.notnull(row["ITM Northing"]):
        lon, lat = transformer.transform(row["ITM Easting"], row["ITM Northing"])
        return pd.Series([lat, lon])
    return pd.Series([None, None])

df[["latitude", "longitude"]] = df.apply(convert_coords, axis=1)

print("Valid latitude:", df["latitude"].notna().sum())

# -------------------------
# FILTER TO OBJECTIONS
# -------------------------

df_obs = df[(df["n_observation_letters"] > 0) &
            (df["latitude"].notna())].copy()

print("Applications plotted (with objections):", len(df_obs))

# -------------------------
# MAP
# -------------------------

m = folium.Map(location=[53.35, -6.26], zoom_start=12)

for _, row in df_obs.iterrows():

    objections = int(row["n_observation_letters"])

    folium.CircleMarker(
        location=[row["latitude"], row["longitude"]],
        radius=5 + objections,
        popup=(
            f"<b>{row['application_number']}</b><br>"
            f"Objections: {objections}<br>"
            f"{row['Development Address']}"
        ),
        color="red",
        fill=True,
        fill_opacity=0.8
    ).add_to(m)

m.save("dcc_applications_with_objections.html")

print("\nMap saved to: dcc_applications_with_objections.html")
