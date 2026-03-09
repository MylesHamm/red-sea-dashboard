#!/usr/bin/env python3
"""
Refresh ACLED data files for the dashboard.

Run locally or via GitHub Actions to keep JSON fallback files current.
Usage: python refresh_data.py
"""
import json
import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

import config
import data_service

def refresh_acled_events():
    """Fetch latest ACLED events and update the JSON fallback."""
    print("Fetching main ACLED events...")
    data_service._acled_token = None  # Clear any stale token

    # Clear cache to force fresh fetch
    cache_path = config.CACHE_DIR / "acled_events.json"
    if cache_path.exists():
        cache_path.unlink()

    events = data_service.fetch_acled_events()

    # Only update if we got real API data (not fallback)
    if len(events) > 1000:  # Fallback CSV has ~362, real data has 17K+
        out = config.DATA_DIR / "acled_events.json"
        out.write_text(json.dumps(events, default=str))
        print(f"  Updated acled_events.json: {len(events)} events ({out.stat().st_size / 1024 / 1024:.1f} MB)")
        return True
    else:
        print(f"  Skipped update — only {len(events)} events (likely fallback data)")
        return False


def refresh_iran_events():
    """Fetch latest Iran ACLED events and update the JSON fallback."""
    print("Fetching Iran ACLED events...")
    data_service._acled_token = None

    cache_path = config.CACHE_DIR / "iran_events.json"
    if cache_path.exists():
        cache_path.unlink()

    events = data_service.fetch_iran_events()

    if len(events) > 100:  # Real data has 600+
        out = config.DATA_DIR / "iran_events.json"
        out.write_text(json.dumps(events, default=str))
        print(f"  Updated iran_events.json: {len(events)} events ({out.stat().st_size / 1024:.0f} KB)")
        return True
    else:
        print(f"  Skipped update — only {len(events)} events (likely fallback data)")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("ACLED Data Refresh")
    print("=" * 60)

    updated = False
    updated |= refresh_acled_events()
    updated |= refresh_iran_events()

    if updated:
        print("\nData files updated. Commit and push to deploy.")
    else:
        print("\nNo updates — ACLED API may be unreachable.")

    sys.exit(0 if updated else 1)
