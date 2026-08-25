"""
Polls the Lyft GBFS API to ingest Bay Wheels vehicle status data.
"""

import logging
import sys
import requests

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(process)d - %(message)s"
)

GBFS_URL = "https://gbfs.lyft.com/gbfs/2.3/bay/en/free_bike_status.json"

def fetch_fleet_status():
    # Identify as a standard browser to bypass basic anti-bot WAF rules
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        # 10s timeout prevents the daemon thread from hanging during network partitions
        response = requests.get(GBFS_URL, headers=headers, timeout=10)
        response.raise_for_status()
        
        payload = response.json()
        
        # GBFS v2.3 schema specifically nests the vehicle array under data.bikes
        bikes = payload.get("data", {}).get("bikes", [])
        
        logging.info(f"Successfully fetched {len(bikes)} vehicles from Bay Wheels.")
        return bikes

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch GBFS data: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_fleet_status()