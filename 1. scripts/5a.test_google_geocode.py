import requests
import os

# -------------------------
# LOAD API KEY
# -------------------------

API_KEY = os.getenv("GOOGLE_GEOCODING_API_KEY")

if not API_KEY:
    raise ValueError("Google API key not found in environment variable.")

print("API key loaded successfully.")

# -------------------------
# TEST ADDRESS
# -------------------------

test_address = "89 Lindsay Road, Glasnevin, Dublin 9, Ireland"

url = "https://maps.googleapis.com/maps/api/geocode/json"
params = {
    "address": test_address,
    "key": API_KEY
}

response = requests.get(url, params=params)
data = response.json()

print("Status:", data["status"])

if data["status"] == "OK":
    location = data["results"][0]["geometry"]["location"]
    print("Latitude:", location["lat"])
    print("Longitude:", location["lng"])
else:
    print("Full response:")
    print(data)
