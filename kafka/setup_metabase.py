"""
One-time Metabase setup script.
Creates the admin account and adds the PostgreSQL database connection.
Run after starting Docker Compose and waiting ~20s for Metabase to initialize.
"""

import json
import time
import sys

import requests

METABASE_URL = "http://localhost:3000"


def wait_for_metabase():
    for i in range(30):
        try:
            r = requests.get(f"{METABASE_URL}/api/health", timeout=5)
            if r.status_code == 200:
                return True
        except requests.exceptions.ConnectionError:
            pass
        print(f"Waiting for Metabase to start... ({i + 1}/30)")
        time.sleep(2)
    return False


def setup():
    if not wait_for_metabase():
        print("Metabase did not start in time.")
        sys.exit(1)

    props = requests.get(f"{METABASE_URL}/api/session/properties").json()
    token = props.get("setup_token")
    if not token:
        print("Metabase already has a user. Skipping setup.")
        return

    # Create admin user
    r = requests.post(
        f"{METABASE_URL}/api/setup",
        json={
            "token": token,
            "user": {
                "first_name": "Admin",
                "last_name": "Workshop",
                "email": "admin@workshop.local",
                "password": "workshop123!",
            },
            "prefs": {"site_name": "Happiness Predictions", "allow_tracking": False},
        },
    )
    r.raise_for_status()
    print("Admin user created: admin@workshop.local / workshop123!")

    # Get session token
    session = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": "admin@workshop.local", "password": "workshop123!"},
    ).json()["id"]

    # Add PostgreSQL database
    r = requests.post(
        f"{METABASE_URL}/api/database",
        headers={"X-Metabase-Session": session},
        json={
            "engine": "postgres",
            "name": "Happiness Predictions DB",
            "details": {
                "host": "postgres",
                "port": 5432,
                "dbname": "happiness_predictions",
                "user": "workshop",
                "password": "workshop",
            },
            "is_full_sync": True,
            "is_on_demand": False,
        },
    )
    r.raise_for_status()
    print("PostgreSQL database connected.")
    print("Setup complete! Dashboard at http://localhost:3000")


if __name__ == "__main__":
    setup()
