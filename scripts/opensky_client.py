import json
import logging
import os
import time
from typing import Optional
import requests

logger = logging.getLogger(__name__)

JKIA_BBOX = {
    "lamin": -2.5,
    "lomin": 35.5,
    "lamax": 1.5,
    "lomax": 38.5,
}

OPENSKY_TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
OPENSKY_STATES_URL = "https://opensky-network.org/api/states/all"

STATE_VECTOR_FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source",
]

def load_credentials(credentials_path: str) -> dict:
    with open(credentials_path, "r") as f:
        return json.load(f)

def get_bearer_token(credentials: dict) -> Optional[str]:
    payload = {
        "grant_type": "client_credentials",
        "client_id": credentials["clientId"],
        "client_secret": credentials["clientSecret"],
    }
    try:
        response = requests.post(OPENSKY_TOKEN_URL, data=payload, timeout=15)
        response.raise_for_status()
        token_data = response.json()
        logger.info("Successfully obtained OpenSky bearer token")
        return token_data["access_token"]
    except requests.RequestException as e:
        logger.error(f"Failed to obtain bearer token: {e}")
        raise

def poll_aircraft_states(token: str, poll_timestamp: int) -> list[dict]:
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "lamin": JKIA_BBOX["lamin"],
        "lomin": JKIA_BBOX["lomin"],
        "lamax": JKIA_BBOX["lamax"],
        "lomax": JKIA_BBOX["lomax"],
    }

    try:
        response = requests.get(OPENSKY_STATES_URL, headers=headers, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        logger.error(f"OpenSky API request failed: {e}")
        raise

    states = data.get("states") or []
    records = []
    for state_vector in states:
        if len(state_vector) < len(STATE_VECTOR_FIELDS):
            continue
        record = dict(zip(STATE_VECTOR_FIELDS, state_vector))
        record["sensors"] = json.dumps(record.get("sensors")) if record.get("sensors") else None
        record["poll_timestamp"] = poll_timestamp
        records.append(record)

    logger.info(f"Polled {len(records)} aircraft states at timestamp {poll_timestamp}")
    return records

def poll_with_retry(credentials_path: str, max_retries: int = 3) -> tuple[list[dict], int]:
    credentials = load_credentials(credentials_path)
    poll_timestamp = int(time.time())

    for attempt in range(max_retries):
        try:
            token = get_bearer_token(credentials)
            records = poll_aircraft_states(token, poll_timestamp)
            return records, poll_timestamp
        except Exception as e:
            if attempt < max_retries - 1:
                wait = 2 ** attempt * 5
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait}s")
                time.sleep(wait)
            else:
                logger.error(f"All {max_retries} attempts failed")
                raise