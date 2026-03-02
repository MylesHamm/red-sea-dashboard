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
MYLES_DATASET_PATH = DATA_DIR / "myles_dataset_final.csv"
HOUTHI_EXCEL_PATH = DATA_DIR / "HouthiData_Updated.xlsx"
HOUTHI_CSV_PATH = DATA_DIR / "HouthiDataARCVisuals.csv"

# --- API Keys (from environment variables) ---
EIA_API_KEY = os.environ.get("EIA_API_KEY", "")
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")

# ACLED OAuth Credentials
ACLED_USERNAME = os.environ.get("ACLED_USERNAME", "")
ACLED_PASSWORD = os.environ.get("ACLED_PASSWORD", "")

# --- API Endpoints ---
ACLED_TOKEN_URL = "https://acleddata.com/oauth/token"
ACLED_DATA_URL = "https://acleddata.com/api/acled/read"
EIA_BASE_URL = "https://api.eia.gov/v2"

# --- Cache TTL (seconds) ---
CACHE_TTL_ACLED = 86400       # 24 hours
CACHE_TTL_BRENT = 3600        # 1 hour
CACHE_TTL_YFINANCE = 3600     # 1 hour
CACHE_TTL_FRED = 86400 * 7    # 7 days (monthly data)

# --- Server Settings ---
HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 8000))
AUTO_REFRESH_MINUTES = 30
