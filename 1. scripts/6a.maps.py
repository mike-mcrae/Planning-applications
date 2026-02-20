import pandas as pd
import folium
from folium.plugins import HeatMap

# =========================
# FILE PATH
# =========================

INPUT_PATH = "/Users/mikemcrae/Documents/GitHub/Planning applications/0. data/DCC_objections_geocoded.csv"

# =========================
# LOAD DATA
# =========================

df = pd.read_csv(INPUT_PATH)

# Force numeric lat/long
df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
df["n_observation_letters"] = pd.to_numeric(
    df["n_observation_letters"], errors="coerce"
).fillna(0)

# Drop missing coords
df = df.dropna(subset=["latitude", "longitude"])

print("Points loaded:", len(df))

# =========================
# 1️⃣ RAW DOT MAP
# =========================

m_dots = folium.Map(location=[53.35, -6.26], zoom_start=12)

for _, row in df.iterrows():

    objections = int(row["n_observation_letters"])

    folium.CircleMarker(
        location=[row["latitude"], row["longitude"]],
        radius=4 + objections,
        popup=(
            f"<b>{row['application_number']}</b><br>"
            f"Objections: {objections}<br>"
            f"{row['Development Address']}"
        ),
        color="red",
        fill=True,
        fill_opacity=0.7
    ).add_to(m_dots)

m_dots.save("dcc_objections_raw_dots.html")

print("Raw dot map saved.")


# =========================
# 2️⃣ HEATMAP
# =========================

m_heat = folium.Map(location=[53.35, -6.26], zoom_start=12)

# Weight by number of objections
heat_data = [
    [row["latitude"], row["longitude"], row["n_observation_letters"]]
    for _, row in df.iterrows()
]

HeatMap(
    heat_data,
    radius=15,
    blur=20,
    max_zoom=13
).add_to(m_heat)

m_heat.save("dcc_objections_heatmap.html")

print("Heatmap saved.")

from folium.plugins import MarkerCluster
import numpy as np

# =========================
# 3️⃣ CLUSTER MAP
# =========================

m_cluster = folium.Map(location=[53.35, -6.26], zoom_start=12)
marker_cluster = MarkerCluster().add_to(m_cluster)

for _, row in df.iterrows():

    objections = int(row["n_observation_letters"])

    folium.Marker(
        location=[row["latitude"], row["longitude"]],
        popup=(
            f"<b>{row['application_number']}</b><br>"
            f"Objections: {objections}<br>"
            f"{row['Development Address']}"
        )
    ).add_to(marker_cluster)

m_cluster.save("dcc_objections_clustered.html")
print("Cluster map saved.")


# =========================
# 4️⃣ CLEAN PROPORTIONAL CIRCLES
# =========================

m_prop = folium.Map(location=[53.35, -6.26], zoom_start=12)

max_obs = df["n_observation_letters"].max()

for _, row in df.iterrows():

    # scaled circle size (sqrt scaling is better visually)
    size = 3 + 10 * np.sqrt(row["n_observation_letters"] / max_obs)

    folium.Circle(
        location=[row["latitude"], row["longitude"]],
        radius=size * 50,   # meters (not pixels)
        popup=f"Objections: {int(row['n_observation_letters'])}",
        color="darkred",
        fill=True,
        fill_opacity=0.4
    ).add_to(m_prop)

m_prop.save("dcc_objections_proportional.html")
print("Proportional circle map saved.")


# =========================
# 5️⃣ GRID / HEX-LIKE AGGREGATION MAP
# =========================

# Create simple grid bins
grid_size = 0.01  # ~1km in Dublin

df["lat_bin"] = (df["latitude"] // grid_size) * grid_size
df["lon_bin"] = (df["longitude"] // grid_size) * grid_size

grid = df.groupby(["lat_bin", "lon_bin"]).agg({
    "n_observation_letters": "sum"
}).reset_index()

m_grid = folium.Map(location=[53.35, -6.26], zoom_start=12)

for _, row in grid.iterrows():

    folium.Rectangle(
        bounds=[
            [row["lat_bin"], row["lon_bin"]],
            [row["lat_bin"] + grid_size, row["lon_bin"] + grid_size]
        ],
        color=None,
        fill=True,
        fill_opacity=min(row["n_observation_letters"] / grid["n_observation_letters"].max(), 1),
    ).add_to(m_grid)

m_grid.save("dcc_objections_grid.html")
print("Grid intensity map saved.")

