import json
import requests
from datetime import datetime
import time

CREDENTIALS_FILE = "credentials/credentials.json"  
try:
    with open(CREDENTIALS_FILE, "r") as f:
        creds = json.load(f)
    CLIENT_ID = creds["clientId"]
    CLIENT_SECRET = creds["clientSecret"]
    print("Credentials loaded successfully.")
except Exception as e:
    print(f"Error reading credentials.json → {e}")
    exit(1)

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

payload = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
}

print("\nRequesting access token...")
try:
    resp = requests.post(TOKEN_URL, data=payload, timeout=15)
    resp.raise_for_status()
    token_data = resp.json()
    ACCESS_TOKEN = token_data["access_token"]
    print("Success → Access token received.")
except Exception as e:
    print(f"Authentication failed → {e}")
    print("Response:", resp.text if 'resp' in locals() else "No response")
    exit(1)

BBOX = {
    "lamin": -2.5,   
    "lomin": 35.8,   
    "lamax": 0.5,    
    "lomax": 38.5,   
}

STATES_URL = "https://opensky-network.org/api/states/all"

headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "User-Agent": "JKIA-Flight-Monitor-Test/1.0",
}

params = {
    "lamin": BBOX["lamin"],
    "lomin": BBOX["lomin"],
    "lamax": BBOX["lamax"],
    "lomax": BBOX["lomax"],   
}

print(f"\nQuerying aircraft states in box: {BBOX}")
print(f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")

try:
    r = requests.get(STATES_URL, headers=headers, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()

    states = data.get("states", [])
    time_val = data.get("time")

    print(f"\nAPI response time: {time_val} (unix timestamp)")
    print(f"Number of aircraft found: {len(states)}")

    if len(states) == 0:
        print("\n→ No aircraft currently visible in this bounding box.")
        print("Possible reasons:")
        print("  • Low ADS-B coverage in East Africa/Kenya airspace")
        print("  • Off-peak time (try during arrival/departure windows: ~06:00–10:00 or 16:00–22:00 UTC)")
        print("  • Bounding box too tight/loose — try widening it slightly")
        print("  • Rate limit / temporary API issue")
    else:
        print("\nSample aircraft (first 3):")
        for aircraft in states[:3]:
            icao24 = aircraft[0]
            callsign = (aircraft[1] or "").strip()
            origin_country = aircraft[2]
            longitude = aircraft[5]
            latitude = aircraft[6]
            altitude = aircraft[7]
            velocity = aircraft[9]
            print(f"  • ICAO24: {icao24} | Callsign: {callsign} | From: {origin_country}")
            print(f"    Pos: ({latitude:.4f}, {longitude:.4f}) | Alt: {altitude} m | Speed: {velocity} m/s")

except requests.exceptions.HTTPError as e:
    print(f"HTTP Error {e.response.status_code}: {e.response.text}")
except Exception as e:
    print(f"Request failed: {e}")

print("\nDone.")