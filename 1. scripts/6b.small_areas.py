import os
import pandas as pd
import geopandas as gpd
import numpy as np
import json
from shapely.geometry import Point
import folium
import matplotlib.pyplot as plt

# =====================================================
# PATHS
# =====================================================

MASTER_APPS_PATH = "/Users/mikemcrae/Documents/GitHub/Planning applications/1. scripts/outputs/applications_master_with_obs.csv"
SA_SHAPEFILE = "/Users/mikemcrae/Documents/GitHub/Planning applications/0. data/Small_Area_National_Statistical_Boundaries_2022_Ungeneralised_view_2205995009404967982/SMALL_AREA_2022.shp"
SA_POP_JSON = "/Users/mikemcrae/Documents/GitHub/Planning applications/0. data/cso/SAP2022T1T1ASA.20260219T220214.json"
ED_POP_FILE = "/Users/mikemcrae/Documents/GitHub/Planning applications/0. data/cso/CensusHub2022_T9_1_ED_7008835473864658512.csv"

OUTPUT_DIR = "/Users/mikemcrae/Documents/GitHub/Planning applications/2. maps"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =====================================================
# LOAD APPLICATION DATA
# =====================================================

print("Loading applications...")
df = pd.read_csv(MASTER_APPS_PATH)

df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
df = df.dropna(subset=["latitude", "longitude"])

geometry = [Point(xy) for xy in zip(df["longitude"], df["latitude"])]
gdf_apps = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

# =====================================================
# LOAD SMALL AREAS
# =====================================================

gdf_sa = gpd.read_file(SA_SHAPEFILE).to_crs("EPSG:4326")
gdf_sa = gdf_sa[gdf_sa["COUNTY_ENG"] == "DUBLIN CITY"].copy()

# =====================================================
# LOAD SA POPULATION
# =====================================================

with open(SA_POP_JSON) as f:
    pop_data = json.load(f)

size = pop_data["size"]
values = pop_data["value"]
dims = pop_data["dimension"]

sa_index = dims["C04172V04943"]["category"]["index"]
age_index = dims["C03737V04485"]["category"]["index"]
sex_index = dims["C03738V04487"]["category"]["index"]

values_array = np.array(values).reshape(size)

age_total = age_index.index("AGET")
sex_total = sex_index.index("B")

pop_vector = values_array[0, 0, :, age_total, sex_total]

df_pop_sa = pd.DataFrame({
    "SA_GUID_21": sa_index,
    "POP2022_SA": pop_vector
})

gdf_sa = gdf_sa.merge(df_pop_sa, on="SA_GUID_21", how="left")

# =====================================================
# SPATIAL JOIN
# =====================================================

joined = gpd.sjoin(gdf_apps, gdf_sa, how="left", predicate="within")

# =====================================================
# SA AGGREGATION
# =====================================================

sa_counts = joined.groupby("SA_GUID_21").agg(
    total_apps=("application_number", "count"),
    apps_with_objection=("has_observation", "sum"),
    total_letters=("n_observation_letters", "sum")
).reset_index()

gdf_sa = gdf_sa.merge(sa_counts, on="SA_GUID_21", how="left")
gdf_sa[["total_apps", "apps_with_objection", "total_letters"]] = \
    gdf_sa[["total_apps", "apps_with_objection", "total_letters"]].fillna(0)

gdf_sa["prop_with_objection"] = np.where(
    gdf_sa["total_apps"] > 0,
    gdf_sa["apps_with_objection"] / gdf_sa["total_apps"],
    0
)

gdf_sa["letters_per_1000"] = np.where(
    gdf_sa["POP2022_SA"] > 0,
    (gdf_sa["total_letters"] / gdf_sa["POP2022_SA"]) * 1000,
    0
)

# =====================================================
# ED AGGREGATION
# =====================================================

ed_numeric = gdf_sa.groupby("ED_GUID").agg(
    total_apps=("total_apps", "sum"),
    apps_with_objection=("apps_with_objection", "sum"),
    total_letters=("total_letters", "sum")
).reset_index()

ed_geometry = gdf_sa[["ED_GUID", "geometry"]].dissolve(by="ED_GUID").reset_index()

gdf_ed = ed_geometry.merge(ed_numeric, on="ED_GUID", how="left")

gdf_ed[["total_apps", "apps_with_objection", "total_letters"]] = \
    gdf_ed[["total_apps", "apps_with_objection", "total_letters"]].fillna(0)

# Load ED population
df_ed_pop = pd.read_csv(ED_POP_FILE)
df_ed_pop = df_ed_pop[df_ed_pop["COUNTY"] == "DUBLIN"]
df_ed_pop = df_ed_pop[["ED_GUID", "Total Population (Normalisation)"]]
df_ed_pop.rename(columns={
    "Total Population (Normalisation)": "POP2022_ED"
}, inplace=True)

gdf_ed = gdf_ed.merge(df_ed_pop, on="ED_GUID", how="left")

gdf_ed["prop_with_objection"] = np.where(
    gdf_ed["total_apps"] > 0,
    gdf_ed["apps_with_objection"] / gdf_ed["total_apps"],
    0
)

gdf_ed["letters_per_1000"] = np.where(
    gdf_ed["POP2022_ED"] > 0,
    (gdf_ed["total_letters"] / gdf_ed["POP2022_ED"]) * 1000,
    0
)

# =====================================================
# BINNING FUNCTION (ZERO SEPARATED)
# =====================================================

def cap_and_bin(series, upper_percentile=0.95, bins=6):
    s = series.copy()
    non_zero = s[s > 0]

    if len(non_zero) == 0:
        return pd.Series(0, index=series.index)

    cap = non_zero.quantile(upper_percentile)
    s = np.minimum(s, cap)

    binned = pd.Series(0, index=series.index)
    binned.loc[s > 0] = pd.qcut(
        s[s > 0],
        q=bins,
        labels=False,
        duplicates="drop"
    ) + 1

    return binned

for gdf in [gdf_sa, gdf_ed]:
    gdf["apps_bin"] = cap_and_bin(gdf["total_apps"])
    gdf["letters_bin"] = cap_and_bin(gdf["letters_per_1000"])
    gdf["prop_bin"] = cap_and_bin(gdf["prop_with_objection"])

# =====================================================
# INTERACTIVE MAP FUNCTION
# =====================================================

def save_map(gdf, column, key, title, filename):
    m = folium.Map(tiles="cartodbpositron")
    bounds = gdf.total_bounds
    m.fit_bounds([[bounds[1], bounds[0]],
                  [bounds[3], bounds[2]]])

    folium.Choropleth(
        geo_data=gdf.__geo_interface__,
        data=gdf,
        columns=[key, column],
        key_on=f"feature.properties.{key}",
        fill_color="YlOrRd",
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name=title
    ).add_to(m)

    m.save(os.path.join(OUTPUT_DIR, filename))
    print("Saved", filename)

# =====================================================
# SAVE ALL INTERACTIVE MAPS
# =====================================================

print("Saving interactive maps...")

# SA
save_map(gdf_sa, "apps_bin", "SA_GUID_21", "SA Total Applications", "map_SA_total_apps.html")
save_map(gdf_sa, "prop_bin", "SA_GUID_21", "SA Proportion with Objection", "map_SA_prop.html")
save_map(gdf_sa, "letters_bin", "SA_GUID_21", "SA Letters per 1000 Population", "map_SA_letters.html")

# ED
save_map(gdf_ed, "apps_bin", "ED_GUID", "ED Total Applications", "map_ED_total_apps.html")
save_map(gdf_ed, "prop_bin", "ED_GUID", "ED Proportion with Objection", "map_ED_prop.html")
save_map(gdf_ed, "letters_bin", "ED_GUID", "ED Letters per 1000 Population", "map_ED_letters.html")

# =====================================================
# STATIC SIDE-BY-SIDE GRID
# =====================================================

fig, axes = plt.subplots(2, 3, figsize=(18, 12))

gdf_sa.plot(column="apps_bin", cmap="OrRd", ax=axes[0,0])
axes[0,0].set_title("SA Total Apps")

gdf_sa.plot(column="prop_bin", cmap="OrRd", ax=axes[0,1])
axes[0,1].set_title("SA Prop")

gdf_sa.plot(column="letters_bin", cmap="OrRd", ax=axes[0,2])
axes[0,2].set_title("SA Letters/1000")

gdf_ed.plot(column="apps_bin", cmap="OrRd", ax=axes[1,0])
axes[1,0].set_title("ED Total Apps")

gdf_ed.plot(column="prop_bin", cmap="OrRd", ax=axes[1,1])
axes[1,1].set_title("ED Prop")

gdf_ed.plot(column="letters_bin", cmap="OrRd", ax=axes[1,2])
axes[1,2].set_title("ED Letters/1000")

for ax in axes.flatten():
    ax.axis("off")

plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, "SA_vs_ED_all_maps.png"), dpi=300)
plt.close()

print("All maps saved.")
