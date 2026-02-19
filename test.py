import json
import requests

API_KEY     = "917fed820e507d7aed1eb0bc763ac2cc1f8d1f4c9451f294a54e8fb2608f9cac"
LOCATION_ID = 2851582
headers     = {"X-API-Key": API_KEY}

# 1. Raw sensor metadata
meta    = requests.get(f"https://api.openaq.org/v3/locations/{LOCATION_ID}", headers=headers).json()
sensors = meta["results"][0]["sensors"]
print("=== SENSORS ===")
for s in sensors:
    print(f"  id={s['id']}  name={s['parameter']['name']}  units={s['parameter']['units']}")

# 2. Raw latest readings
data = requests.get(f"https://api.openaq.org/v3/locations/{LOCATION_ID}/latest", headers=headers).json()
print("\n=== LATEST READINGS (raw) ===")
for r in data.get("results", []):
    print(json.dumps(r, indent=2))