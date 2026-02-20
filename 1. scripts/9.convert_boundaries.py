import geopandas as gpd
from pathlib import Path

# =====================================
# Resolve project root automatically
# =====================================

BASE_DIR = Path(__file__).resolve().parent.parent

SA_SHP = BASE_DIR / "0. data" / "Small_Area_National_Statistical_Boundaries_2022_Ungeneralised_view_2205995009404967982" / "SMALL_AREA_2022.shp"

OUTPUT_DIR = BASE_DIR / "data"
OUTPUT_DIR.mkdir(exist_ok=True)

SA_OUT = OUTPUT_DIR / "dublin_small_areas.geojson"
ED_OUT = OUTPUT_DIR / "dublin_electoral_divisions.geojson"

# =====================================
# Load shapefile
# =====================================

print("Loading shapefile from:")
print(SA_SHP)

if not SA_SHP.exists():
    raise FileNotFoundError(f"Shapefile not found at {SA_SHP}")

gdf = gpd.read_file(SA_SHP)

print("Filtering Dublin City...")
gdf = gdf[gdf["COUNTY_ENG"] == "DUBLIN CITY"].copy()

gdf = gdf.to_crs("EPSG:4326")

# =====================================
# Simplify geometry
# =====================================

print("Simplifying geometry...")
gdf["geometry"] = gdf["geometry"].simplify(0.0005, preserve_topology=True)

# =====================================
# Save Small Areas
# =====================================

print("Saving Small Areas GeoJSON...")
gdf.to_file(SA_OUT, driver="GeoJSON")

# =====================================
# Dissolve to ED
# =====================================

print("Creating Electoral Divisions...")
gdf_ed = gdf.dissolve(by="ED_GUID").reset_index()

gdf_ed.to_file(ED_OUT, driver="GeoJSON")

print("Done.")
print("Saved to:", OUTPUT_DIR)
