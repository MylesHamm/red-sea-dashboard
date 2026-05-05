"""
Dashboard Configuration - API Keys & Settings
Keys are loaded from environment variables or a local .env file.
"""
import os
from pathlib import Path

# Load .env file if it exists (for local development)
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    with open(_env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip())

# --- File Paths ---
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
CACHE_DIR = BASE_DIR / ".cache"

# Source data files (fallback CSVs)
MYLES_DATASET_PATH = DATA_DIR / "master_dataset.csv"
THESIS_EVENTS_PATH = DATA_DIR / "thesis_events.csv"

# --- API Keys (from environment variables) ---
EIA_API_KEY = os.environ.get("EIA_API_KEY", "")
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")

# Financial Modeling Prep — intraday commodity quotes (Brent / WTI),
# energy equity prices, real-time forex. Free tier handles ~250 calls/day,
# more than enough for the dashboard's 60s polling cycle. Used to upgrade
# the hero Brent KPI from EIA daily-settlement to real intraday and to
# add an energy-equity context panel.
FMP_API_KEY = os.environ.get("FMP_API_KEY", "")

# ACLED OAuth Credentials
ACLED_USERNAME = os.environ.get("ACLED_USERNAME", "")
ACLED_PASSWORD = os.environ.get("ACLED_PASSWORD", "")

# --- API Endpoints ---
ACLED_TOKEN_URL = "https://acleddata.com/oauth/token"
ACLED_DATA_URL = "https://acleddata.com/api/acled/read"
EIA_BASE_URL = "https://api.eia.gov/v2"

# --- Cache TTL (seconds) ---
CACHE_TTL_ACLED = 86400       # 24 hours
CACHE_TTL_BRENT = 86400       # 24 hours (daily data)
CACHE_TTL_YFINANCE = 86400    # 24 hours (daily data)
CACHE_TTL_FRED = 86400 * 7    # 7 days (monthly data)

# --- Server Settings ---
HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 8000))
AUTO_REFRESH_MINUTES = 30


# ─── Domain Constants ──────────────────────────────────────────────────────
#
# Single source of truth for values that show up on the dashboard. The frontend
# reads these via /api/constants (assets/constants.js) so updating one number
# here propagates everywhere — no more hunting for hardcoded mbd values or
# anchor dates scattered across hydrate.js / charts.js / data_service.py.

# Conflict timeline anchors
HAMAS_ATTACK_DATE  = "2023-10-07"   # timeline scrubber START
HOUTHI_PHASE_START = "2023-12-01"   # Bab el-Mandeb maritime campaign begins
WAR_ONSET_DATE     = "2026-02-28"   # US-Iran war begins (Operation Epic Fury)

# Thesis observation window — locked for the GARCH/OLS regression.
# Charts in §05/§06/§08 are filtered to this range; tooltips disclose it.
THESIS_WINDOW_START = "2023-10-02"
THESIS_WINDOW_END   = "2025-09-30"
THESIS_WINDOW_N     = 505

# EIA reference flows through the threatened chokepoints (mbd, slow-moving
# macro values from EIA's annual chokepoint analysis). Used by the OIL FLOW
# AT RISK KPI and the per-chokepoint card flow stat. Not live AIS — these
# represent the structural amount at risk under a closure.
CHOKEPOINT_REF_FLOW_MBD = {
    "hormuz": 21.0,
    "bab":     8.2,
    "suez":    5.5,
}

# EIA STEO 2026 forecast for world liquids consumption (mbd). Denominator
# for "% of global supply at risk" calculation.
GLOBAL_LIQUIDS_MBD = 102.0

# Per-chokepoint radius (km) for "events near this chokepoint" counts.
# Hormuz is widest because the operationally relevant zone covers Persian
# Gulf + Gulf of Oman — a 300km radius would only catch events directly
# in the strait.
CHOKEPOINT_RADIUS_KM = {
    "hormuz": 650,
    "bab":    400,
    "suez":   300,
}

# Bounding boxes for "incident in this chokepoint zone" filter — larger
# than the AIS kill rings because ACLED geocodes events to launch/impact
# sites which can be inland. Format: (top_left, bottom_right).
INCIDENT_BOUNDING_BOXES = {
    "hormuz": ((30.5, 50.0), (22.5, 60.5)),  # Persian Gulf + Gulf of Oman + Iran/UAE/Oman coast
    "bab":    ((20.0, 39.0), (10.0, 49.0)),  # South Red Sea + Yemen + Gulf of Aden
    "suez":   ((33.0, 30.0), (27.0, 36.0)),  # Suez Canal + Sinai
}

# Actor-substring matches that we accept anywhere on the globe for each
# chokepoint (so a Houthi attack coded inland in Yemen still surfaces for
# Bab even if its coordinates fall outside the bbox). Substring-insensitive,
# matched against ACLED actor1+actor2.
INCIDENT_ACTOR_HINTS = {
    "hormuz": ("irgc", "iranian navy", "military forces of iran", "islamic revolutionary guard"),
    "bab":    ("houthi", "ansar allah"),
    "suez":   (),
}

# Keyword sets for tagging curated war-timeline + news events to a chokepoint
# when their lat/lon doesn't fall inside the bbox. Word-boundary matched,
# case-insensitive against title + description.
#
# Each chokepoint gets its OWN event stream — no cross-theater spillover.
# Earlier, broadening the sets to include "iran" / "khamenei" / "hormuz" in
# Bab + Suez made every Iran-war news article match all three, so the cards
# all collapsed to the same count (16 / 16 / 16 in the last screenshot).
# That hid the reality that the active war is centered on Hormuz; Bab and
# Suez have their own dynamics (Houthi pause, Suez transit metrics) and
# their cards should reflect that, not echo Hormuz.
INCIDENT_KEYWORDS = {
    # Hormuz: Iran war + Persian Gulf shipping incidents.
    "hormuz": (
        "hormuz", "persian gulf", "strait of hormuz", "irgc", "kharg", "qeshm",
        "bushehr", "bandar abbas", "fujairah", "gulf of oman", "iranian navy",
        "khamenei", "tehran", "iran", "uae", "abu dhabi",
        "us strikes iran", "us-iran", "us iran",
    ),
    # Bab: Houthi maritime ops, Red Sea, Yemen. Strictly direct — Iran-war
    # spillover is real economically but it's not "an event happening at
    # Bab", it's an event happening at Hormuz that may affect Bab risk.
    # The card's transit-decline % captures the spillover; this count is
    # for the direct event stream.
    "bab": (
        "bab el-mandeb", "bab al-mandab", "red sea", "houthi", "houthis",
        "ansar allah", "yemen", "salalah", "djibouti", "gulf of aden",
    ),
    # Suez: Canal-direct events. Almost always sparse — the meaningful Suez
    # signal is transit volume (PortWatch), not direct-attack events.
    "suez": (
        "suez", "sinai", "egyptian canal",
    ),
}

# Threat classification thresholds (transit decline % vs pre-war baseline).
# Ordered most-severe first; first match wins.
THREAT_TIERS = [
    ("critical", 50.0),   # ≥50% decline
    ("high",     25.0),   # ≥25%
    ("elevated", 10.0),   # ≥10%
    ("safe",     0.0),    # everything else
]
