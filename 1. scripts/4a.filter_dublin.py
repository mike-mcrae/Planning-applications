import pandas as pd

INPUT_PATH = "/Users/mikemcrae/Documents/GitHub/Planning applications/0. data/IrishPlanningApplications_2359694955245257726.csv"
OUTPUT_PATH = "/Users/mikemcrae/Documents/GitHub/Planning applications/0. data/IrishPlanningApplications_DublinCityCouncil.csv"

# -------------------------
# LOAD SAFELY (robust parser)
# -------------------------

apps = pd.read_csv(
    INPUT_PATH,
    sep=",",
    engine="python",          # tolerant parser
    on_bad_lines="skip"       # skip malformed rows
)

print("Total rows loaded:", len(apps))
print("Columns detected:")
print(apps.columns.tolist())

# -------------------------
# FILTER
# -------------------------

apps_dcc = apps[
    apps["Planning Authority"].astype(str).str.strip() == "Dublin City Council"
].copy()

print("Dublin City Council rows:", len(apps_dcc))

# -------------------------
# SAVE CLEAN FILE
# -------------------------

apps_dcc.to_csv(OUTPUT_PATH, index=False)

print("\nFiltered file saved to:")
print(OUTPUT_PATH)
