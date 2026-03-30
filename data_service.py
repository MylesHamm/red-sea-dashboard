"""
Data Service Layer - API integration with caching and CSV fallback.
Handles: ACLED, EIA (Brent + SPR), yfinance (DXY, OVX), FRED (China BCI)
"""
import bisect
import json
import os
import re
import time
import logging
import threading
from pathlib import Path
from collections import defaultdict, OrderedDict
from datetime import datetime, timedelta
from typing import Optional, List, Dict

import pandas as pd
import numpy as np
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ensure cache directory exists
config.CACHE_DIR.mkdir(exist_ok=True)

# Browser-like headers to avoid WAF blocks from cloud IPs
_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
}


# ─── Conflict Theater Location Lookup ────────────────────────────────────────
# Maps lowercase keywords → (lat, lon, canonical_name) for geocoding headlines

CONFLICT_THEATER_LOCATIONS = {
    # Iran
    "tehran": (35.6892, 51.3890, "Tehran, Iran"),
    "isfahan": (32.6546, 51.6680, "Isfahan, Iran"),
    "natanz": (33.5103, 51.9250, "Natanz, Iran"),
    "fordow": (34.7564, 51.0596, "Fordow, Iran"),
    "bushehr": (28.9234, 50.8203, "Bushehr, Iran"),
    "bandar abbas": (27.1865, 56.2808, "Bandar Abbas, Iran"),
    "kharg island": (29.2333, 50.3167, "Kharg Island, Iran"),
    "tabriz": (38.0800, 46.2919, "Tabriz, Iran"),
    "shiraz": (29.5918, 52.5837, "Shiraz, Iran"),
    "qom": (34.6401, 50.8764, "Qom, Iran"),
    "minab": (27.1050, 57.0786, "Minab, Iran"),
    "chabahar": (25.2919, 60.6430, "Chabahar, Iran"),
    "shahran": (35.75, 51.30, "Shahran, Tehran, Iran"),
    "abadan": (30.3392, 48.3043, "Abadan, Iran"),
    # Strait / Gulf
    "strait of hormuz": (26.5667, 56.2500, "Strait of Hormuz"),
    "hormuz": (26.5667, 56.2500, "Strait of Hormuz"),
    "persian gulf": (25.2854, 55.3500, "Persian Gulf"),
    # Oman
    "salalah": (17.0151, 54.0924, "Salalah, Oman"),
    "muscat": (23.5880, 58.3829, "Muscat, Oman"),
    # Gulf States
    "riyadh": (24.7136, 46.6753, "Riyadh, Saudi Arabia"),
    "jeddah": (21.4858, 39.1925, "Jeddah, Saudi Arabia"),
    "shaybah": (22.5167, 54.0000, "Shaybah, Saudi Arabia"),
    "dubai": (25.2048, 55.2708, "Dubai, UAE"),
    "abu dhabi": (24.4539, 54.3773, "Abu Dhabi, UAE"),
    "manama": (26.2285, 50.5860, "Manama, Bahrain"),
    "bahrain": (26.0667, 50.5577, "Bahrain"),
    "doha": (25.2854, 51.5310, "Doha, Qatar"),
    "qatar": (25.2854, 51.5310, "Qatar"),
    "kuwait": (29.3759, 47.9774, "Kuwait City, Kuwait"),
    # Iraq / Levant
    "baghdad": (33.3152, 44.3661, "Baghdad, Iraq"),
    "basra": (30.5085, 47.7804, "Basra, Iraq"),
    "beirut": (33.8938, 35.5018, "Beirut, Lebanon"),
    "damascus": (33.5138, 36.2765, "Damascus, Syria"),
    # Israel
    "tel aviv": (32.0853, 34.7818, "Tel Aviv, Israel"),
    "jerusalem": (31.7683, 35.2137, "Jerusalem, Israel"),
    "haifa": (32.7940, 34.9896, "Haifa, Israel"),
    "ovda": (29.9402, 34.9358, "Ovda Airbase, Israel"),
    # US / West
    "washington": (38.9072, -77.0369, "Washington, DC"),
    "pentagon": (38.8719, -77.0563, "Pentagon, VA"),
    "geneva": (46.2044, 6.1432, "Geneva, Switzerland"),
    # Waterways
    "red sea": (20.0, 38.0, "Red Sea"),
    "suez": (29.9668, 32.5498, "Suez Canal, Egypt"),
    "bab el-mandeb": (12.5833, 43.3333, "Bab el-Mandeb"),
    "bab el mandeb": (12.5833, 43.3333, "Bab el-Mandeb"),
    "gulf of aden": (12.5, 45.0, "Gulf of Aden"),
    # Yemen
    "aden": (12.7855, 45.0187, "Aden, Yemen"),
    "sanaa": (15.3694, 44.1910, "Sanaa, Yemen"),
    "hodeidah": (14.7979, 42.9541, "Hodeidah, Yemen"),
}

# Pre-sorted keys longest-first for greedy matching
_LOCATION_KEYS_SORTED = sorted(CONFLICT_THEATER_LOCATIONS.keys(), key=len, reverse=True)

# ─── Cache Helpers ───────────────────────────────────────────────────────────

def _cache_path(key: str) -> Path:
    return config.CACHE_DIR / f"{key}.json"


def _read_cache(key: str, ttl: int) -> Optional[dict]:
    path = _cache_path(key)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        if time.time() - data.get("_ts", 0) < ttl:
            return data.get("payload")
    except Exception:
        pass
    return None


def _write_cache(key: str, payload):
    path = _cache_path(key)
    data = json.dumps({"_ts": time.time(), "payload": payload}, default=str)
    # Atomic write: write to temp file then rename to avoid corruption on crash
    tmp = str(path) + ".tmp"
    try:
        Path(tmp).write_text(data, encoding="utf-8")
        os.replace(tmp, str(path))
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ─── ACLED API ───────────────────────────────────────────────────────────────

_acled_token = None
_acled_token_expires = 0
_acled_token_lock = threading.Lock()
_iran_fetch_error = None  # Store last error for debugging


def _get_acled_token() -> str:
    global _acled_token, _acled_token_expires
    with _acled_token_lock:
        if _acled_token and time.time() < _acled_token_expires:
            return _acled_token

        if not config.ACLED_USERNAME or not config.ACLED_PASSWORD:
            raise ValueError("ACLED credentials not configured (set ACLED_USERNAME and ACLED_PASSWORD env vars)")

        resp = requests.post(
            config.ACLED_TOKEN_URL,
            data={
                "username": config.ACLED_USERNAME,
                "password": config.ACLED_PASSWORD,
                "grant_type": "password",
                "client_id": "acled",
            },
            headers={**_BROWSER_HEADERS, "Content-Type": "application/x-www-form-urlencoded"},
            timeout=8,
        )
        if not resp.ok:
            raise requests.HTTPError(
                f"{resp.status_code} for {resp.url} | resp={resp.text[:300]}",
                response=resp,
            )
        token_data = resp.json()
        _acled_token = token_data["access_token"]
        _acled_token_expires = time.time() + token_data.get("expires_in", 86400) - 300
        logger.info("ACLED OAuth token acquired")
        return _acled_token


_EVENT_COLUMNS = ["event_id_cnty", "event_date", "event_type", "sub_event_type",
                   "actor1", "actor2", "country", "location", "latitude", "longitude",
                   "notes", "fatalities", "tags", "source", "source_scale"]

_ACLED_FIELDS = "|".join(_EVENT_COLUMNS)
_ACLED_DATE_RANGE = "2023-10-01|2026-12-31"


def _df_to_event_records(df: pd.DataFrame) -> List[dict]:
    """Convert a DataFrame to a list of event dicts (vectorized, no iterrows)."""
    col_map = {c: c.lower() for c in df.columns}
    df = df.rename(columns=col_map)
    str_cols = [c for c in _EVENT_COLUMNS if c not in ("latitude", "longitude", "fatalities")]
    for c in str_cols:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str)
    if "latitude" in df.columns:
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    if "longitude" in df.columns:
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    if "fatalities" in df.columns:
        df["fatalities"] = pd.to_numeric(df["fatalities"], errors="coerce").fillna(0).astype(int)
    records = df[[c for c in _EVENT_COLUMNS if c in df.columns]].to_dict("records")
    # Replace NaN lat/lon with None for JSON
    for r in records:
        if "latitude" in r and (r["latitude"] is None or pd.isna(r["latitude"])):
            r["latitude"] = None
        if "longitude" in r and (r["longitude"] is None or pd.isna(r["longitude"])):
            r["longitude"] = None
    return records

# Red Sea maritime keywords for filtering regional events.
# Only very specific terms — avoids false matches from generic conflict words.
_MARITIME_KEYWORDS = [
    "houthi", "ansar allah", "red sea", "bab el-mandeb", "bab al-mandab",
    "gulf of aden", "maritime", "shipping", "vessel", "tanker", "cargo ship",
    "oil tanker", "commercial ship", "merchant vessel", "container ship",
    "suez canal", "usns", "uss ",
    "piracy", "hijack", "sea route", "waterway", "blockade",
    "coast guard", "naval blockade", "naval operation",
]


def _paginated_acled_fetch(token: str, params: dict, label: str, max_pages: int = 10) -> List[dict]:
    """Fetch paginated ACLED results."""
    results = []
    for page in range(1, max_pages + 1):
        p = {**params, "page": page}
        resp = requests.get(
            config.ACLED_DATA_URL,
            headers={**_BROWSER_HEADERS, "Authorization": f"Bearer {token}"},
            params=p,
            timeout=10,
        )
        resp.raise_for_status()
        batch = resp.json().get("data", [])
        if not batch:
            break
        results.extend(batch)
        logger.info(f"ACLED {label}: page {page} ({len(batch)} events)")
        if len(batch) < int(params.get("limit", 5000)):
            break
    return results


def _is_maritime_relevant(event: dict) -> bool:
    """Check if an event is relevant to Red Sea / maritime operations."""
    text = f"{event.get('notes', '')} {event.get('actor1', '')} {event.get('actor2', '')}".lower()
    return any(kw in text for kw in _MARITIME_KEYWORDS)


def fetch_acled_events() -> List[dict]:
    """Fetch comprehensive Houthi/Red Sea events from ACLED with multi-query approach.

    Strategy:
    1. All Yemen events (primary conflict zone)
    2. Houthi/Ansar Allah actor events globally (maritime attacks outside Yemen)
    3. Red Sea regional countries filtered for maritime relevance
    """
    cached = _read_cache("acled_events", config.CACHE_TTL_ACLED)
    if cached:
        logger.info("ACLED: serving from cache")
        return cached

    try:
        token = _get_acled_token()
        all_events = []
        seen_ids = set()

        def _add_unique(events):
            added = 0
            for e in events:
                eid = e.get("event_id_cnty")
                if eid and eid not in seen_ids:
                    all_events.append(e)
                    seen_ids.add(eid)
                    added += 1
            return added

        base_params = {
            "_format": "json",
            "event_date": _ACLED_DATE_RANGE,
            "event_date_where": "BETWEEN",
            "fields": _ACLED_FIELDS,
            "limit": 5000,
        }

        # All queries run in parallel for speed
        regional_countries = ["Saudi Arabia", "Djibouti", "Eritrea", "Oman", "Somalia", "Egypt", "Sudan", "Jordan", "Israel"]

        def _fetch_yemen():
            return "Yemen", _paginated_acled_fetch(token, {**base_params, "country": "Yemen"}, "Yemen"), False

        def _fetch_houthi():
            return "Houthi-actor", _paginated_acled_fetch(token, {
                **base_params, "actor1": "Houthi", "actor1_where": "LIKE",
            }, "Houthi-actor"), False

        def _fetch_ansar():
            return "AnsarAllah-actor", _paginated_acled_fetch(token, {
                **base_params, "actor1": "Ansar Allah", "actor1_where": "LIKE",
            }, "AnsarAllah-actor"), False

        def _fetch_regional(country):
            regional = _paginated_acled_fetch(token, {**base_params, "country": country}, country)
            maritime = [e for e in regional if _is_maritime_relevant(e)]
            return country, maritime, True

        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = [
                pool.submit(_fetch_yemen),
                pool.submit(_fetch_houthi),
                pool.submit(_fetch_ansar),
            ]
            futures.extend(pool.submit(_fetch_regional, c) for c in regional_countries)

            for future in as_completed(futures):
                label, events_list, is_regional = future.result()
                n = _add_unique(events_list)
                if is_regional:
                    logger.info(f"ACLED {label}: {len(events_list)} maritime, {n} new")
                else:
                    logger.info(f"ACLED {label}: {n} unique events")

        if all_events:
            _write_cache("acled_events", all_events)
            logger.info(f"ACLED: total {len(all_events)} unique events fetched and cached")
            return all_events

    except Exception as e:
        logger.warning(f"ACLED API failed: {e}")

    return _load_acled_fallback()


def _load_acled_fallback() -> List[dict]:
    """Load ACLED data from JSON fallback, then CSV files."""
    # Try JSON fallback first (pre-fetched comprehensive dataset)
    json_path = config.DATA_DIR / "acled_events.json"
    if json_path.exists():
        try:
            events = json.loads(json_path.read_text())
            logger.info(f"ACLED fallback: loaded {len(events)} events from acled_events.json")
            return events
        except Exception as e:
            logger.warning(f"ACLED JSON fallback failed: {e}")

    # Fall back to thesis events CSV
    logger.info("ACLED: falling back to thesis events CSV")
    if config.THESIS_EVENTS_PATH.exists():
        try:
            df = pd.read_csv(config.THESIS_EVENTS_PATH)
            col_map = {c: c.lower() for c in df.columns}
            df.rename(columns=col_map, inplace=True)
            records = _df_to_event_records(df)
            logger.info(f"ACLED CSV fallback: loaded {len(records)} events from {config.THESIS_EVENTS_PATH.name}")
            return records
        except Exception as e:
            logger.warning(f"Failed to load thesis events CSV: {e}")
    return []


# ─── Thesis Dataset (726 Verified Maritime Events) ───────────────────────────

_thesis_events_cache: Optional[List[dict]] = None

def load_thesis_events() -> List[dict]:
    """Load the 726 ACLED-verified maritime events analyzed in the thesis."""
    global _thesis_events_cache
    if _thesis_events_cache is not None:
        return _thesis_events_cache

    csv_path = config.THESIS_EVENTS_PATH
    if not csv_path.exists():
        logger.warning("No thesis events CSV found, falling back to ACLED")
        return fetch_acled_events()

    try:
        df = pd.read_csv(csv_path)
        col_map = {c: c.lower() for c in df.columns}
        df.rename(columns=col_map, inplace=True)

        # Map CH6 column names to expected schema
        if "date" in df.columns and "event_date" not in df.columns:
            df.rename(columns={"date": "event_date"}, inplace=True)
        if "event_id" in df.columns and "event_id_cnty" not in df.columns:
            df.rename(columns={"event_id": "event_id_cnty"}, inplace=True)

        records = _df_to_event_records(df)
        _thesis_events_cache = records
        logger.info(f"Thesis dataset: loaded {len(records)} events from {csv_path.name}")
        return records
    except Exception as e:
        logger.warning(f"Failed to load thesis events: {e}")
        return fetch_acled_events()


# ─── EIA API v2 (Brent Crude + SPR) ─────────────────────────────────────────

def fetch_brent_prices() -> List[dict]:
    """Fetch daily Brent crude spot prices from EIA API v2."""
    cached = _read_cache("brent_prices", config.CACHE_TTL_BRENT)
    if cached:
        logger.info("Brent: serving from cache")
        return cached

    try:
        resp = requests.get(
            f"{config.EIA_BASE_URL}/petroleum/pri/spt/data",
            params={
                "api_key": config.EIA_API_KEY,
                "frequency": "daily",
                "data[0]": "value",
                "facets[series][]": "RBRTE",
                "sort[0][column]": "period",
                "sort[0][direction]": "asc",
                "start": "2023-10-01",
                "length": 5000,
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        records = [
            {"date": r["period"], "price": float(r["value"])}
            for r in data.get("response", {}).get("data", [])
            if r.get("value") is not None
        ]
        if records:
            records = _supplement_brent_recent(records)
            _write_cache("brent_prices", records)
            logger.info(f"Brent: fetched {len(records)} daily prices (EIA + yfinance)")
            return records
    except Exception as e:
        logger.warning(f"EIA Brent API failed: {e}")

    fallback = _load_brent_csv_fallback()
    if fallback:
        fallback = _supplement_brent_recent(fallback)
    return fallback


def _supplement_brent_recent(eia_records: List[dict]) -> List[dict]:
    """Extend EIA/FRED Brent data with recent prices beyond API reporting lag.

    Data waterfall (each layer only fills gaps left by the one above):
        1. Yahoo Finance direct API  (real-time, but often blocked from servers)
        2. yfinance library           (same data, different client)
        3. FRED DCOILBRENTEU series   (reliable, ~1-day lag, auto-updates daily)
        4. Hardcoded fallback table   (last resort for known war-period dates)
    """
    # Deduplicate by date (keep latest) and sort chronologically
    by_date = {r["date"]: r for r in eia_records}
    last_date = max(by_date.keys()) if by_date else "2023-10-01"

    # Hardcoded war-period settlements from CNBC / Reuters / Bloomberg.
    # Used ONLY when ALL live sources (EIA, Yahoo, FRED) fail for a given date.
    fallback_prices = {
        "2026-03-02": 77.24,   # First trading day after war started Feb 28 (Sat)
        "2026-03-03": 83.28,   # Brent surges as shipping suspended
        "2026-03-04": 81.56,   # Slight pullback amid heavy strikes
        "2026-03-05": 88.59,   # Brent surges on insurance withdrawal, 500+ missiles
        "2026-03-06": 95.74,   # Analysts warn $100+; Iran strikes Gulf states
        # Mar 7 (Sat) and Mar 8 (Sun) — no settlement
        "2026-03-09": 98.96,   # Brent settles +3.4% from Friday
        "2026-03-10": 87.80,   # Sharp pullback (-11.3%) as Trump signals war "very complete"
        "2026-03-11": 91.98,   # CNBC: Brent +4.76% as IEA releases 400M bbl reserves
        "2026-03-12": 100.46,  # CNBC: First close above $100 since Aug 2022 (+9.22%)
        "2026-03-13": 103.14,  # CNBC: Brent +2.67%; oil above $100 for second day
        # Mar 14 (Sat) and Mar 15 (Sun) — no settlement
        "2026-03-16": 101.04,  # EIA/FRED spot price
        "2026-03-17": 103.42,  # CNBC: +3.2% as allies refuse Hormuz escort
        "2026-03-18": 107.38,  # CNBC: +3.8% amid Hormuz shutdown fears
        "2026-03-19": 108.65,  # CNBC: Hit $119 intraday; settled +1.18% after Netanyahu comments
        "2026-03-20": 110.96,  # Iraq force majeure on Basra crude; supply disruptions widen
        # Mar 21 (Sat) and Mar 22 (Sun) — no settlement
        "2026-03-23": 96.07,   # Sharp pullback on IRGC Hormuz 'tollbooth' de-escalation signals
        "2026-03-24": 100.09,  # Partial recovery; China/India negotiate Hormuz passage
        "2026-03-25": 103.42,  # Kuwait airport drone attack; regional spread
        "2026-03-26": 108.01,  # Iran rejects talks; IRGC confirms tollbooth system
        "2026-03-27": 105.32,  # Marines arrive; humanitarian corridor eases fears slightly
        "2026-03-28": 112.57,  # Houthis fire missiles at Israel; 82nd Airborne deploys
    }

    # ── Layers 1-3: Run ALL live sources in parallel ──────────────────────────
    # Yahoo Finance almost always fails from Render cloud IPs, so don't wait
    # sequentially. FRED is the most reliable server-side source.

    def _try_yahoo_direct():
        """Layer 1: Yahoo Finance direct API."""
        try:
            last_dt = datetime.strptime(last_date, "%Y-%m-%d")
            period1 = int(last_dt.timestamp())
            period2 = int((datetime.now() + timedelta(days=1)).timestamp())
            yf_url = (
                f"https://query1.finance.yahoo.com/v8/finance/chart/BZ=F"
                f"?period1={period1}&period2={period2}&interval=1d"
            )
            yf_resp = requests.get(yf_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=3)
            yf_resp.raise_for_status()
            yf_data = yf_resp.json()
            result = yf_data.get("chart", {}).get("result", [])
            prices = {}
            if result:
                timestamps = result[0].get("timestamp", [])
                closes = result[0]["indicators"]["quote"][0].get("close", [])
                for ts, close in zip(timestamps, closes):
                    if close is None:
                        continue
                    d = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
                    if d > last_date:
                        prices[d] = {"date": d, "price": round(float(close), 2)}
            return prices
        except Exception as e:
            logger.warning(f"Yahoo Finance API failed: {e}")
            return {}

    def _try_yfinance_lib():
        """Layer 2: yfinance library."""
        try:
            import yfinance as yf
            df = yf.download("BZ=F", start=last_date, progress=False)
            if df.empty:
                return {}
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df_valid = df[df["Close"].notna()].copy()
            df_valid["_date"] = df_valid.index.strftime("%Y-%m-%d")
            df_valid = df_valid[df_valid["_date"] > last_date]
            return {d: {"date": d, "price": round(float(p), 2)} for d, p in zip(df_valid["_date"], df_valid["Close"])}
        except Exception as e:
            logger.warning(f"Brent yfinance failed: {e}")
            return {}

    def _try_fred():
        """Layer 3: FRED DCOILBRENTEU (most reliable, ~1-day lag)."""
        try:
            from fredapi import Fred
            fred = Fred(api_key=config.FRED_API_KEY)
            series = fred.get_series("DCOILBRENTEU", observation_start=last_date)
            prices = {}
            for idx, val in series.items():
                if pd.notna(val):
                    d = idx.strftime("%Y-%m-%d")
                    if d > last_date:
                        prices[d] = {"date": d, "price": round(float(val), 2)}
            return prices
        except Exception as e:
            logger.warning(f"FRED Brent (DCOILBRENTEU) failed: {e}")
            return {}

    # Run all 3 sources in parallel — first to return wins for each date
    with ThreadPoolExecutor(max_workers=3) as pool:
        f_yahoo = pool.submit(_try_yahoo_direct)
        f_yf = pool.submit(_try_yfinance_lib)
        f_fred = pool.submit(_try_fred)

    # Merge results: FRED first (most reliable), then overlay Yahoo/yfinance
    for source_prices in [f_fred.result(), f_yf.result(), f_yahoo.result()]:
        for d, rec in source_prices.items():
            if d not in by_date:
                by_date[d] = rec

    live_added = sum(1 for d in by_date if d > last_date)
    if live_added:
        logger.info(f"Brent: {live_added} live prices supplemented after {last_date}")

    # ── Layer 4: Hardcoded fallback (last resort) ────────────────────────────
    for date_str, price in fallback_prices.items():
        if date_str not in by_date:
            by_date[date_str] = {"date": date_str, "price": price}
            logger.info(f"Brent: using fallback price for {date_str}: ${price}")
    if len(by_date) > len(eia_records):
        logger.info(f"Brent: supplemented {len(by_date) - len(eia_records)} prices after {last_date}")

    return sorted(by_date.values(), key=lambda r: r["date"])


def _load_brent_csv_fallback() -> List[dict]:
    logger.info("Brent: falling back to CSV")
    if config.MYLES_DATASET_PATH.exists():
        df = pd.read_csv(config.MYLES_DATASET_PATH)
        date_col = df.columns[0]
        df.rename(columns={date_col: "Date"}, inplace=True)
        df["Date"] = pd.to_datetime(df["Date"])
        df_valid = df[df["Brent_Price"].notna()].copy()
        records = [
            {"date": d.strftime("%Y-%m-%d"), "price": float(p)}
            for d, p in zip(df_valid["Date"], df_valid["Brent_Price"])
        ]
        return records
    return []


def fetch_spr_data() -> List[dict]:
    """Fetch SPR stock levels from EIA API v2."""
    cached = _read_cache("spr_data", config.CACHE_TTL_BRENT)
    if cached:
        return cached

    try:
        resp = requests.get(
            f"{config.EIA_BASE_URL}/petroleum/stoc/wstk/data",
            params={
                "api_key": config.EIA_API_KEY,
                "frequency": "weekly",
                "data[0]": "value",
                "facets[product][]": "EPC0",
                "facets[process][]": "SAX",
                "sort[0][column]": "period",
                "sort[0][direction]": "asc",
                "start": "2023-10-01",
                "length": 5000,
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        records = [
            {"date": r["period"], "value": float(r["value"])}
            for r in data.get("response", {}).get("data", [])
            if r.get("value") is not None
        ]
        if records:
            _write_cache("spr_data", records)
            logger.info(f"SPR: fetched {len(records)} weekly data points")
            return records
    except Exception as e:
        logger.warning(f"EIA SPR API failed: {e}")
    return []


# ─── yfinance (DXY, OVX) ────────────────────────────────────────────────────

def fetch_yfinance_series(ticker: str, cache_key: str) -> List[dict]:
    """Fetch daily time series from Yahoo Finance."""
    cached = _read_cache(cache_key, config.CACHE_TTL_YFINANCE)
    if cached:
        logger.info(f"yfinance {ticker}: serving from cache")
        return cached

    try:
        import yfinance as yf
        df = yf.download(ticker, start="2023-10-01", progress=False)
        if df.empty:
            raise ValueError(f"No data returned for {ticker}")

        # Handle multi-level columns from yfinance
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df_valid = df[df["Close"].notna()].copy()
        records = [
            {"date": idx.strftime("%Y-%m-%d"), "value": round(float(close), 4)}
            for idx, close in zip(df_valid.index, df_valid["Close"])
        ]
        if records:
            _write_cache(cache_key, records)
            logger.info(f"yfinance {ticker}: fetched {len(records)} data points")
            return records
    except Exception as e:
        logger.warning(f"yfinance {ticker} failed: {e}")
    return []


def fetch_dxy() -> List[dict]:
    """DXY: try yfinance first, then FRED DTWEXBGS (broad trade-weighted dollar) as fallback."""
    result = fetch_yfinance_series("DX-Y.NYB", "dxy")
    if result:
        return result
    # FRED fallback — broad trade-weighted dollar index (close proxy for DXY)
    try:
        from fredapi import Fred
        fred = Fred(api_key=config.FRED_API_KEY)
        series = fred.get_series("DTWEXBGS", observation_start="2023-10-01")
        records = [
            {"date": idx.strftime("%Y-%m-%d"), "value": round(float(val), 4)}
            for idx, val in series.items() if pd.notna(val)
        ]
        if records:
            _write_cache("dxy", records)
            logger.info(f"FRED DTWEXBGS (DXY proxy): fetched {len(records)} data points")
            return records
    except Exception as e:
        logger.warning(f"FRED DXY fallback failed: {e}")
    return []


def fetch_ovx() -> List[dict]:
    """OVX: try yfinance first, then FRED OVXCLS as fallback."""
    result = fetch_yfinance_series("^OVX", "ovx")
    if result:
        return result
    # FRED fallback — CBOE Crude Oil ETF Volatility Index
    try:
        from fredapi import Fred
        fred = Fred(api_key=config.FRED_API_KEY)
        series = fred.get_series("OVXCLS", observation_start="2023-10-01")
        records = [
            {"date": idx.strftime("%Y-%m-%d"), "value": round(float(val), 4)}
            for idx, val in series.items() if pd.notna(val)
        ]
        if records:
            _write_cache("ovx", records)
            logger.info(f"FRED OVXCLS: fetched {len(records)} data points")
            return records
    except Exception as e:
        logger.warning(f"FRED OVX fallback failed: {e}")
    return []


# ─── FRED API (China BCI) ───────────────────────────────────────────────────

def fetch_china_pmi() -> List[dict]:
    """Fetch China business confidence from FRED."""
    cached = _read_cache("china_pmi", config.CACHE_TTL_FRED)
    if cached:
        return cached

    try:
        from fredapi import Fred
        fred = Fred(api_key=config.FRED_API_KEY)
        series = fred.get_series("BSCICP03CNM665S", observation_start="2023-10-01")
        records = [
            {"date": idx.strftime("%Y-%m-%d"), "value": round(float(val), 4)}
            for idx, val in series.items()
            if pd.notna(val)
        ]
        if records:
            _write_cache("china_pmi", records)
            logger.info(f"FRED China BCI: fetched {len(records)} data points")
            return records
    except Exception as e:
        logger.warning(f"FRED API failed: {e}")
    return []


# ─── Master Dataset (CSV Backbone) ──────────────────────────────────────────

def load_master_dataset() -> dict:
    """Load the master dataset CSV and return as structured JSON."""
    cached = _read_cache("master_dataset", 1800)  # 30 min cache
    if cached:
        return cached

    if not config.MYLES_DATASET_PATH.exists():
        logger.error(f"Master dataset not found: {config.MYLES_DATASET_PATH}")
        return {"timeseries": [], "kpis": {}, "price_windows": {}, "correlation": []}

    df = pd.read_csv(config.MYLES_DATASET_PATH)
    date_col = df.columns[0]
    df.rename(columns={date_col: "Date"}, inplace=True)
    df["Date"] = pd.to_datetime(df["Date"])
    df.sort_values("Date", inplace=True)

    # Time series records — vectorized (avoids slow iterrows)
    col_map = {
        "Brent_Price": ("brent_price", 2, False),
        "Daily_Volatility": ("daily_volatility", 4, False),
        "DXY": ("dxy", 2, False),
        "OVX": ("ovx", 2, False),
        "SPR_Release_Volume": ("spr_release_volume", 4, False),
        "China_PMI": ("china_pmi", 2, True),        # True = also exclude zeros
        "Baker_Hughes_Rigs": ("baker_hughes_rigs", 1, True),
    }
    int_cols = {
        "WeeklyAttackFreq": "weekly_attacks",
        "daily_attacks": "daily_attacks",
        "tanker_attacks": "tanker_attacks",
        "chokepoint_attacks": "chokepoint_attacks",
        "fatalities": "fatalities_count",
        "OPEC_Dummy": "opec_dummy",
        "RussiaUkraine_Dummy": "russia_ukraine_dummy",
        "OPEC_Decision": "opec_decision",
        "RussiaUkraine_Attacks": "russia_ukraine_attacks",
        "IranIsrael_Escalation": "iran_israel_escalation",
    }
    ts = pd.DataFrame({"date": df["Date"].dt.strftime("%Y-%m-%d")})
    for src, (dst, rnd, excl_zero) in col_map.items():
        if src in df.columns:
            vals = df[src].round(rnd)
            if excl_zero:
                vals = vals.where(df[src] != 0)
            ts[dst] = vals
        else:
            ts[dst] = np.nan
    for src, dst in int_cols.items():
        ts[dst] = df[src].fillna(0).astype(int) if src in df.columns else 0

    # Convert to list of dicts, replacing NaN/numpy types with JSON-safe Python types
    def _clean(v):
        if v is None:
            return None
        if isinstance(v, (np.floating, float)):
            return None if np.isnan(v) else round(float(v), 6)
        if isinstance(v, (np.integer, int)):
            return int(v)
        return v
    timeseries = [{k: _clean(v) for k, v in row.items()} for row in ts.to_dict("records")]

    # KPIs
    valid_prices = df["Brent_Price"].dropna()

    # Use cached Brent price if available (non-blocking); fall back to CSV last-row
    live_brent = _read_cache("brent_prices", config.CACHE_TTL_BRENT)
    if live_brent and len(live_brent) >= 2:
        live_latest = live_brent[-1]["price"]
        live_prev = live_brent[-2]["price"]
        latest_brent = round(live_latest, 2)
        brent_change = round(live_latest - live_prev, 2)
    else:
        latest_brent = round(float(valid_prices.iloc[-1]), 2)
        brent_change = round(float(valid_prices.iloc[-1] - valid_prices.iloc[-2]), 2) if len(valid_prices) > 1 else 0

    # Use cached DXY (non-blocking); fall back to CSV last-row
    live_dxy = _read_cache("dxy", config.CACHE_TTL_YFINANCE)
    latest_dxy = round(live_dxy[-1]["value"], 2) if live_dxy else (
        round(float(df["DXY"].dropna().iloc[-1]), 2) if df["DXY"].dropna().shape[0] > 0 else None
    )

    # Use cached OVX (non-blocking); fall back to CSV last-row
    live_ovx = _read_cache("ovx", config.CACHE_TTL_YFINANCE)
    latest_ovx = round(live_ovx[-1]["value"], 2) if live_ovx else (
        round(float(df["OVX"].dropna().iloc[-1]), 2) if df["OVX"].dropna().shape[0] > 0 else None
    )

    kpis = {
        "avg_brent_price": round(float(valid_prices.mean()), 2),
        "latest_brent_price": latest_brent,
        "brent_price_change": brent_change,
        "peak_volatility": round(float(df["Daily_Volatility"].max()), 4),
        "max_weekly_attacks": int(df["WeeklyAttackFreq"].max()),
        "latest_dxy": latest_dxy,
        "latest_ovx": latest_ovx,
        "total_trading_days": 505,  # Regression observations (506 price days minus 1 for return calc)
    }

    # Price windows (event study: T-2 to T+5)
    price_cols = ["Price_T-2", "Price_T-1", "Price_T0", "Price_T+1", "Price_T+2", "Price_T+3", "Price_T+4", "Price_T+5"]
    attack_rows = df[df["WeeklyAttackFreq"] > 0]
    price_windows = {}
    for col in price_cols:
        if col in df.columns:
            values = attack_rows[col].replace(0, np.nan).dropna()
            price_windows[col] = round(float(values.mean()), 2) if len(values) > 0 else 0

    # Correlation matrix
    corr_cols = ["Brent_Price", "Daily_Volatility", "WeeklyAttackFreq", "DXY", "OVX",
                 "OPEC_Dummy", "RussiaUkraine_Dummy", "IranIsrael_Escalation",
                 "China_PMI", "Baker_Hughes_Rigs", "SPR_Release_Volume"]
    available_cols = [c for c in corr_cols if c in df.columns]
    corr_df = df[available_cols].replace(0, np.nan).dropna(how="all").corr()
    correlation = {
        "labels": list(corr_df.columns),
        "matrix": [[round(float(v), 3) if pd.notna(v) else 0 for v in row] for row in corr_df.values],
    }

    result = {
        "timeseries": timeseries,
        "kpis": kpis,
        "price_windows": price_windows,
        "correlation": correlation,
    }
    _write_cache("master_dataset", result)
    return result


# ─── Hypothesis Results (hardcoded from notebook) ───────────────────────────

# ─── Iran Events (Current Events Tab) ────────────────────────────────────

def fetch_iran_events() -> List[dict]:
    """Fetch Iran-related events from ACLED API with cache."""
    global _iran_fetch_error
    cached = _read_cache("iran_events", 3600)  # 1-hour cache
    if cached:
        logger.info("Iran events: serving from cache")
        _iran_fetch_error = None
        return cached

    try:
        token = _get_acled_token()
        iran_fields = "event_id_cnty|event_date|event_type|sub_event_type|actor1|actor2|location|latitude|longitude|notes|fatalities|tags"

        def _fetch_iran_country():
            results = []
            for page in range(1, 8):  # Cap at 7 pages (35k events max — more than enough)
                resp = requests.get(
                    config.ACLED_DATA_URL,
                    headers={**_BROWSER_HEADERS, "Authorization": f"Bearer {token}"},
                    params={"_format": "json", "country": "Iran",
                            "event_date": "2025-01-01|2026-12-31", "event_date_where": "BETWEEN",
                            "fields": iran_fields, "limit": 5000, "page": page},
                    timeout=10,
                )
                resp.raise_for_status()
                batch = resp.json().get("data", [])
                if not batch:
                    break
                results.extend(batch)
                if len(batch) < 5000:
                    break
            logger.info(f"Iran events (country): {len(results)} events")
            return results

        def _fetch_iran_bilateral(actor1, actor2):
            resp = requests.get(
                config.ACLED_DATA_URL,
                headers={**_BROWSER_HEADERS, "Authorization": f"Bearer {token}"},
                params={"_format": "json", "actor1": actor1, "actor1_where": "LIKE",
                        "actor2": actor2, "actor2_where": "LIKE",
                        "event_date": "2025-01-01|2026-12-31", "event_date_where": "BETWEEN",
                        "fields": iran_fields, "limit": 5000},
                timeout=10,
            )
            resp.raise_for_status()
            bilateral = resp.json().get("data", [])
            logger.info(f"Iran bilateral ({actor1}→{actor2}): {len(bilateral)} events")
            return bilateral

        # Run all 3 queries in parallel
        with ThreadPoolExecutor(max_workers=3) as pool:
            f_country = pool.submit(_fetch_iran_country)
            f_us_iran = pool.submit(_fetch_iran_bilateral, "United States", "Iran")
            f_iran_us = pool.submit(_fetch_iran_bilateral, "Iran", "United States")

        all_events = []
        seen_ids = set()
        for batch in [f_country.result(), f_us_iran.result(), f_iran_us.result()]:
            for e in batch:
                eid = e.get("event_id_cnty")
                if eid and eid not in seen_ids:
                    all_events.append(e)
                    seen_ids.add(eid)

        _iran_fetch_error = None
        if all_events:
            _write_cache("iran_events", all_events)
            logger.info(f"Iran events: total {len(all_events)} events fetched and cached")
            return all_events
        else:
            _iran_fetch_error = "ACLED returned 0 events for all Iran queries"
            logger.warning(_iran_fetch_error)

    except Exception as e:
        _iran_fetch_error = f"{type(e).__name__}: {e}"
        logger.warning(f"Iran events API failed: {_iran_fetch_error}")

    # Fallback: load from pre-fetched JSON file
    fallback = _load_iran_json_fallback()
    if fallback:
        _iran_fetch_error = None  # Clear error since fallback succeeded
    return fallback


def _load_iran_json_fallback() -> List[dict]:
    """Load Iran events from local JSON fallback file."""
    path = config.DATA_DIR / "iran_events.json"
    if not path.exists():
        logger.warning("Iran events: no JSON fallback file found")
        return []
    try:
        events = json.loads(path.read_text())
        logger.info(f"Iran events: loaded {len(events)} events from JSON fallback")
        return events
    except Exception as e:
        logger.warning(f"Iran events JSON fallback failed: {e}")
        return []


def get_iran_fetch_error() -> Optional[str]:
    return _iran_fetch_error


def get_curated_iran_events() -> List[dict]:
    """Return curated timeline of major US-Iran events (2025-2026) with coordinates."""
    return [
        # ── Phase 1: Maximum Pressure Restored (Jan-Feb 2025) ──
        {"date": "2025-01-20", "title": "Trump Inaugurated, Rescinds Biden-Era Iran Policies", "type": "diplomatic", "description": "Trump signs EO 14148 rescinding 67 Biden-era executive orders including Iran sanctions-related actions.", "severity": 2, "lat": 38.9072, "lon": -77.0369, "location": "Washington, DC"},
        {"date": "2025-02-04", "title": "Trump Signs 'Maximum Pressure' Executive Order", "type": "sanctions", "description": "NSPM-2 restores maximum pressure campaign: Treasury to impose maximum economic pressure, State Dept rescinds sanctions waivers, campaign to drive Iran oil exports to zero.", "severity": 3, "lat": 38.9072, "lon": -77.0369, "location": "Washington, DC"},
        {"date": "2025-02-06", "title": "OFAC Sanctions Iranian Oil Shipping Network", "type": "sanctions", "description": "Treasury's OFAC sanctions international network of parties and vessels facilitating Iranian crude oil shipments to China.", "severity": 2, "lat": 38.9072, "lon": -77.0369, "location": "Washington, DC"},

        # ── Phase 2: Nuclear Talks Begin (Mar-Jun 2025) ──
        {"date": "2025-03-07", "title": "Trump Sends Letter to Khamenei with 2-Month Deadline", "type": "diplomatic", "description": "Trump sends letter via UAE diplomat Anwar Gargash proposing nuclear negotiations, warning of military consequences if rejected.", "severity": 3, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2025-04-12", "title": "Round 1: US-Iran Indirect Talks Begin in Muscat", "type": "diplomatic", "description": "First indirect US-Iran nuclear talks mediated by Oman. US envoy Witkoff and Iranian FM Araghchi in separate rooms. Both sides call talks 'constructive.'", "severity": 2, "lat": 23.5880, "lon": 58.3829, "location": "Muscat, Oman"},
        {"date": "2025-05-11", "title": "Round 4: US Demands Complete Dismantlement", "type": "diplomatic", "description": "Fourth round in Muscat. Witkoff demands complete dismantlement of Natanz, Fordow, and Isfahan. Positions harden.", "severity": 3, "lat": 23.5880, "lon": 58.3829, "location": "Muscat, Oman"},
        {"date": "2025-05-31", "title": "IAEA: Iran Has 400+ kg of 60% Enriched Uranium", "type": "nuclear", "description": "Confidential IAEA report confirms 400+ kg of 60% enriched uranium, enough for ~10 nuclear weapons if further enriched; total stockpile 40x JCPOA limit.", "severity": 4, "lat": 33.5103, "lon": 51.9250, "location": "Natanz, Iran"},

        # ── Phase 3: The Twelve-Day War (Jun 2025) ──
        {"date": "2025-06-13", "title": "Israel Launches 'Operation Rising Lion': Strikes Iran", "type": "military", "description": "Israel launches surprise strikes on Iranian nuclear facilities including Natanz. Prominent military leaders and nuclear scientists assassinated. US-Iran talks suspended.", "severity": 5, "lat": 33.5103, "lon": 51.9250, "location": "Natanz, Iran"},
        {"date": "2025-06-21", "title": "US Launches 'Operation Midnight Hammer'", "type": "military", "description": "125+ aircraft including seven B-2 bombers with GBU-57 bunker busters strike Fordow, Natanz, Isfahan. Tomahawks from submarines. Trump claims facilities 'obliterated.'", "severity": 5, "lat": 34.7564, "lon": 51.0596, "location": "Fordow, Iran"},
        {"date": "2025-06-22", "title": "Iran Retaliates: 550+ Missiles, 1000+ Drones at Israel", "type": "military", "description": "Iran launches over 550 ballistic missiles and 1,000+ drones at Israeli and US targets. Most intercepted by Israel and US.", "severity": 5, "lat": 32.0853, "lon": 34.7818, "location": "Tel Aviv, Israel"},
        {"date": "2025-06-24", "title": "Twelve-Day War Ceasefire Agreed", "type": "diplomatic", "description": "Israel and Iran agree to ceasefire under US pressure, ending the Twelve-Day War.", "severity": 4, "lat": 38.9072, "lon": -77.0369, "location": "Washington, DC"},

        # ── Phase 4: Snapback Sanctions & Isolation (Aug-Oct 2025) ──
        {"date": "2025-08-28", "title": "E3 Triggers JCPOA Snapback Sanctions Mechanism", "type": "sanctions", "description": "UK, France, and Germany invoke JCPOA snapback citing Iran's 'significant non-performance.' 30-day countdown begins.", "severity": 4, "lat": 40.7489, "lon": -73.9680, "location": "New York, NY (UN)"},
        {"date": "2025-09-27", "title": "UN Snapback Sanctions Formally Reimposed on Iran", "type": "sanctions", "description": "All UN sanctions lifted under JCPOA formally reimposed: travel bans, asset freezes, arms embargo, ballistic missile restrictions. EU follows Sept 29.", "severity": 4, "lat": 40.7489, "lon": -73.9680, "location": "New York, NY (UN)"},
        {"date": "2025-10-18", "title": "Iran Officially Terminates the JCPOA", "type": "diplomatic", "description": "Iran declares the JCPOA over on 'Termination Day.' Iran, Russia, and China declare UN sanctions invalid.", "severity": 3, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},

        # ── Phase 5: Protests & Crackdown (Dec 2025 - Jan 2026) ──
        {"date": "2025-12-28", "title": "Massive Anti-Regime Protests Erupt Across Iran", "type": "proxy", "description": "Protests erupt after rial collapses to 1.4M/$1. Tehran Grand Bazaar strikes spread nationwide. 72% food inflation, post-war devastation, snapback sanctions.", "severity": 4, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-01-08", "title": "Iran's Deadliest Crackdown Since 1979", "type": "military", "description": "Security forces launch massive crackdown. Internet fully cut. Firearms and shotguns with metal pellets used against protesters. Thousands reported killed.", "severity": 5, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-01-23", "title": "Trump Announces Naval 'Armada' Heading to Middle East", "type": "military", "description": "Trump announces USS Abraham Lincoln carrier strike group deployment. F/A-18E Super Hornets, F-35C Lightning IIs, guided-missile destroyers.", "severity": 4, "lat": 25.2854, "lon": 55.3500, "location": "Persian Gulf"},

        # ── Phase 6: War Buildup & Hormuz Provocations (Feb 2026) ──
        {"date": "2026-01-30", "title": "IRGC Seizes South Korean Tanker in Strait of Hormuz", "type": "military", "description": "IRGC Navy commandos fast-rope onto South Korean chemical tanker 'Hankuk Chemi II' in Strait of Hormuz, citing 'environmental violations.' Crew of 20 detained at Bandar Abbas. Seoul condemns 'act of piracy.' Mirrors 2021 tanker seizure. Oil markets spike 3.2%.", "severity": 4, "fatalities": 0, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-02-03", "title": "IRGC Attempts to Board US Tanker; Drone Shot Down", "type": "military", "description": "IRGC Navy attempts to intercept US-flagged tanker in Strait of Hormuz. USS McFaul escorts it to safety. F-35C shoots down Iranian Shahed-136 drone.", "severity": 3, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-02-10", "title": "IRGC Fires Warning Shots at Norwegian Tanker Near Hormuz", "type": "military", "description": "IRGC Navy patrol boat fires warning shots across bow of Norwegian-flagged tanker 'Nordic Spirit' in international waters near Hormuz. USS Carney intervenes; IRGC boats withdraw. Norway summons Iranian ambassador. Third Hormuz shipping incident in 11 days.", "severity": 3, "fatalities": 0, "lat": 26.4800, "lon": 56.3000, "location": "Strait of Hormuz"},
        {"date": "2026-02-06", "title": "Round 6: US-Iran Talks Resume in Muscat", "type": "diplomatic", "description": "First talks since June 2025. US delegation: Witkoff, Kushner, CENTCOM commander Adm. Cooper. Iranian FM Araghchi leads. 'Good start.'", "severity": 3, "lat": 23.5880, "lon": 58.3829, "location": "Muscat, Oman"},
        {"date": "2026-02-13", "title": "USS Gerald R. Ford Redeployed; Trump Signals Regime Change", "type": "military", "description": "Ford redirected to Middle East, largest US force posture since 2003 Iraq War. Trump says regime change would be 'best thing that could happen.'", "severity": 4, "lat": 25.2854, "lon": 55.3500, "location": "Persian Gulf"},
        {"date": "2026-02-14", "title": "Pentagon Prepares 'Weeks-Long Sustained Operations'", "type": "military", "description": "US officials confirm military is preparing for sustained operations against Iran lasting weeks.", "severity": 4, "lat": 38.8719, "lon": -77.0563, "location": "Pentagon, VA"},
        {"date": "2026-02-19", "title": "Trump Gives Iran 10-Day Ultimatum", "type": "diplomatic", "description": "Trump tells Iran to reach a 'meaningful' deal within 10-15 days or 'really bad things' will happen. IRGC conducts live-fire Strait of Hormuz drill.", "severity": 5, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-02-24", "title": "F-22s Deployed to Israel; State of the Union Warning", "type": "military", "description": "12 F-22s deployed to Ovda Airbase, the first US offensive weapons in Israel. Trump vows in SOTU that Iran will never have nuclear weapons.", "severity": 4, "lat": 29.9402, "lon": 34.9358, "location": "Ovda Airbase, Israel"},
        {"date": "2026-02-26", "title": "Final Nuclear Talks Fail: No Deal Reached", "type": "diplomatic", "description": "Round 8 in Geneva. US demands: destroy all enrichment sites, surrender uranium, permanent deal, end proxies. Iran refuses missile restrictions. No agreement.", "severity": 4, "lat": 46.2044, "lon": 6.1432, "location": "Geneva, Switzerland"},
        {"date": "2026-02-27", "title": "IAEA Reveals Hidden Uranium; Embassies Evacuate Iran", "type": "nuclear", "description": "IAEA reports 440.9 kg of 60% enriched uranium hidden in Isfahan tunnels. Embassies evacuate. Trump gives go order for Operation Epic Fury from Air Force One.", "severity": 5, "lat": 32.6546, "lon": 51.6680, "location": "Isfahan, Iran"},

        # ── Phase 7: Operation Epic Fury / Iran War (Feb 28+, 2026) ──
        {"date": "2026-02-28", "title": "Operation Epic Fury Begins: US & Israel Strike Iran; Khamenei Killed", "type": "military", "description": "Joint US-Israeli strikes at 2:30 AM EST. ~900 US strikes in 12 hours, 1,000+ targets in 24h. Supreme Leader Khamenei killed in Israeli strikes on Tehran compound. Iran retaliates with dozens of ballistic missiles and drones at Israel and US bases. US Embassy in Kuwait hit.", "severity": 5, "fatalities": 354, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-03-01", "title": "Maersk Suspends Strait of Hormuz Transit; 4 US Soldiers Killed", "type": "proxy", "description": "Maersk suspends all Strait of Hormuz crossings, reroutes around Cape of Good Hope. Tanker transits drop from 24/day to 4. Four US soldiers killed in Kuwait drone strike.", "severity": 5, "fatalities": 4, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-03-02", "title": "IRGC Closes Strait of Hormuz; Hezbollah Enters War", "type": "military", "description": "IRGC officially closes Strait of Hormuz, threatens any ship that passes. 150+ ships anchored outside. Hezbollah fires rockets at Israel; IDF invades southern Lebanon.", "severity": 5, "fatalities": 85, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-03-03", "title": "Global Shipping Suspended; Brent Surges 7.8%", "type": "proxy", "description": "CMA CGM, Hapag-Lloyd, MSC suspend strait transits. IRGC deploys anti-ship missile launchers to Qeshm Island at narrowest Hormuz point. Iranian drones hit Gulf infrastructure. Brent settles at $83.28 (+7.8%).", "severity": 5, "fatalities": 45, "lat": 26.8500, "lon": 55.9000, "location": "Strait of Hormuz"},
        {"date": "2026-03-04", "title": "IRGC Fast Boats Attack Greek Tanker in Hormuz", "type": "military", "description": "IRGC Navy fires RPGs at Greek-flagged VLCC 'Athena Glory' transiting Hormuz under US escort. USS Mason sinks two IRGC boats. First direct naval engagement since war began. Iranian civilian toll passes 1,100. Brent settles at $81.56 (-2.1%).", "severity": 5, "fatalities": 128, "lat": 26.4500, "lon": 56.4000, "location": "Strait of Hormuz"},
        {"date": "2026-03-05", "title": "IRGC Mines Sink Neutral Vessel; Insurance Withdrawn for Hormuz", "type": "military", "description": "Indian-flagged MV Ganges Spirit strikes Iranian mine in Hormuz, first neutral vessel sunk. Lloyd's suspends all Hormuz hull coverage. Iran fires 500+ missiles, 2,000 drones total. NATO intercepts missile over Turkey. Brent at $88.59 (+8.6%, ~24% since war began).", "severity": 5, "fatalities": 154, "lat": 26.5200, "lon": 56.3500, "location": "Strait of Hormuz"},
        {"date": "2026-03-06", "title": "Iran Strikes Gulf States; Brent Surges to $95.74 (+8.1%)", "type": "military", "description": "Iran strikes Saudi Arabia, Kuwait, Qatar, Bahrain, UAE. Missile hits Jerusalem. US Navy SEALs board IRGC minelayer deploying mines in Hormuz shipping lanes; 23 mines neutralized. CENTCOM declares limited maritime corridor. Brent settles at $95.74 (+8.1%).", "severity": 5, "fatalities": 95, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-03-07", "title": "Iranian Submarine Fires Torpedo at US Destroyer", "type": "military", "description": "IRIN Kilo-class submarine fires torpedo at USS Halsey in Gulf of Oman — first submarine attack on US Navy since WWII. Torpedo malfunctions; P-8 Poseidon forces submarine to surface. Iran's missile capability reported down 90% by Pentagon.", "severity": 5, "fatalities": 180, "lat": 25.5000, "lon": 57.5000, "location": "Gulf of Oman"},
        {"date": "2026-03-08", "title": "Brent Breaks $100/bbl; Shahran Oil Depot and Bandar Abbas Hit", "type": "military", "description": "Israel hits Shahran oil depot near Tehran; toxic smoke over capital. US destroys Bandar Abbas naval base — 14 IRGC vessels sunk. CENTCOM: 'IRGC Navy capability eliminated.' Assembly of Experts names Mojtaba Khamenei new Supreme Leader. Brent breaks $100/bbl.", "severity": 5, "fatalities": 285, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-03-09", "title": "Brent Hits $119/bbl Intraday; IRGC Drones Strike Fujairah Tanker", "type": "military", "description": "IRGC Shahed-136 drones strike Japanese VLCC 'Nippon Maru' at Fujairah, world's 2nd-largest bunkering hub; force majeure declared. Saudi intercepts drone toward Shaybah oilfield. Cumulative toll: 2,400+ killed. Brent hits $119/bbl intraday (highest since 2022), settles at $98.96.", "severity": 5, "fatalities": 252, "lat": 25.1288, "lon": 56.3265, "location": "Fujairah, UAE"},
        {"date": "2026-03-10", "title": "Brent Drops 11.3% as Trump Signals War 'Very Complete'", "type": "diplomatic", "description": "Trump tells CBS war is 'very complete, pretty much.' Energy Secretary's false tanker escort claim triggers flash crash. US destroys 16 Iranian minelayers. Brent drops 11.3% to $87.80, largest single-day drop since March 2022.", "severity": 5, "fatalities": 200, "lat": 38.9072, "lon": -77.0369, "location": "Washington, DC"},
        {"date": "2026-03-11", "title": "Iranian Drones Strike Salalah Port, Oman; Shipping Route Threatened", "type": "military", "description": "Iranian drone strike sets fire to Salalah, Oman's largest commercial port — major escalation hitting neutral mediator state. Threatens critical non-Hormuz shipping route. Iran rejects ceasefire. Pentagon: 140 US troops wounded, 7 killed since Feb 28.", "severity": 5, "fatalities": 180, "lat": 17.0151, "lon": 54.0924, "location": "Salalah, Oman"},
        {"date": "2026-03-12", "title": "Brent Surges Past $105; Hormuz Transit Drops to 2 Ships/Day", "type": "proxy", "description": "Brent hits $105.40/bbl. Hormuz transit at 2 ships/day (was 24). IEA declares first 'severe supply disruption' since 2011. Houthis fire missiles at Saudi Ras Tanura oil terminal. IRGC anti-ship missiles hit tanker near Fujairah. 21% of global oil supply disrupted.", "severity": 5, "fatalities": 70, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-03-13", "title": "Mojtaba Khamenei Threatens Permanent Hormuz Closure", "type": "diplomatic", "description": "New Supreme Leader threatens to permanently seal the Strait of Hormuz. Analysts note IRGC Navy largely destroyed since March 8; uncleared sea mines remain primary residual threat.", "severity": 3, "fatalities": 0, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-03-14", "title": "Trump Orders Strikes on Kharg Island; 90% of Iran's Oil Exports", "type": "military", "description": "Trump orders strikes on Kharg Island, which handles 90% of Iran's crude oil exports. Major escalation targeting Iran's economic lifeline. Separate strikes hit Isfahan, damaging UNESCO-listed Chehel Sotoun Palace.", "severity": 5, "fatalities": 15, "lat": 29.2333, "lon": 50.3167, "location": "Kharg Island, Iran"},
        {"date": "2026-03-15", "title": "Iran FM Declares 'Ready for a Long War'; Rejects Ceasefire", "type": "diplomatic", "description": "Iranian FM Araghchi states Tehran 'never sought a ceasefire' and remains ready for a long war. Trump urges world to keep Hormuz open. Bahrain and Saudi Arabia cancel April F1 Grand Prix over safety.", "severity": 4, "fatalities": 0, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-03-16", "title": "Houthis Await Iran Approval to Resume Red Sea Attacks", "type": "proxy", "description": "Reports emerge that Houthis are awaiting Iranian approval to resume Red Sea shipping attacks if Iran's Hormuz control weakens. FM Araghchi rejects ceasefire. Lebanon death toll reaches 850 with 831,000 displaced.", "severity": 4, "fatalities": 50, "lat": 14.0, "lon": 44.0, "location": "Red Sea"},
        {"date": "2026-03-17", "title": "Israel Assassinates Larijani; Iran Retaliates Across Gulf States", "type": "military", "description": "Israel kills top security official Ali Larijani and Basij commander Gholamreza Soleimani. Iran retaliates with strikes at Saudi Arabia, Kuwait, UAE. Multiple missiles hit Tel Aviv, killing at least 2. Broadening regional escalation.", "severity": 5, "fatalities": 5, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-03-18", "title": "South Pars Gas Field Struck; Intelligence Minister Khatib Killed", "type": "military", "description": "Israel strikes South Pars gas field — world's largest natural gas reserve shared with Qatar. Iran takes several South Pars phases offline. Israel also confirms killing Intelligence Minister Esmail Khatib in Tehran, a day after assassinating Larijani and Basij chief Soleimani. Funeral processions for Larijani draw massive crowds in Tehran. Iran rejects ceasefire; Hormuz transit effectively halted.", "severity": 5, "fatalities": 0, "lat": 27.5000, "lon": 52.0000, "location": "South Pars, Persian Gulf"},

        # ── Day 20 (March 19, 2026) ──
        {"date": "2026-03-19", "title": "Brent Hits $115; Strikes on Yazd Airport; Ras Laffan LNG Hub Hit", "type": "military", "description": "Brent crude surges to $115/bbl after Israeli strikes spark Iranian retaliation on Qatar's Ras Laffan LNG hub. US-Israeli strikes hit Yazd Airport. Trump threatens to 'blow up' South Pars if Iran continues attacking Qatar. Saudi Arabia warns it will take military action against Iran. IDF has carried out 7,600+ strikes across Iran. Pentagon requests $200B+ war budget. FM Araghchi warns allies helping reopen Hormuz risk 'complicity in war crimes.'", "severity": 5, "fatalities": 0, "lat": 31.9049, "lon": 54.2825, "location": "Yazd, Iran"},

        # ── Days 21-30 (March 20-29, 2026) ──
        {"date": "2026-03-20", "title": "Iraq Declares Force Majeure on Basra Crude; Brent at $110.96", "type": "proxy", "description": "Iraq declares force majeure on Basra crude exports citing pipeline damage from Iranian retaliatory strikes near Fao Peninsula. Gulf tanker insurance premiums hit record highs. Brent settles at $110.96 as supply disruptions widen beyond Iran.", "severity": 4, "fatalities": 0, "lat": 30.5085, "lon": 47.7804, "location": "Basra, Iraq"},
        {"date": "2026-03-23", "title": "Brent Drops to $96; IRGC Hormuz 'Tollbooth' Rumors Emerge", "type": "diplomatic", "description": "Brent drops sharply to $96.07 as reports emerge that IRGC may allow select nations to transit Hormuz for fees — a de facto 'tollbooth' system. Markets interpret this as partial de-escalation. China and India reportedly negotiating safe passage for their flagged tankers.", "severity": 3, "fatalities": 0, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-03-25", "title": "Kuwait Airport Drone Attack; UNIFIL Peacekeeper Killed in Lebanon", "type": "military", "description": "Iranian-aligned militia drones strike Kuwait International Airport, damaging runway and terminal. One UNIFIL peacekeeper killed in southern Lebanon as Hezbollah-IDF fighting intensifies. Regional conflict spreading to previously neutral states.", "severity": 4, "fatalities": 3, "lat": 29.2266, "lon": 47.9689, "location": "Kuwait City, Kuwait"},
        {"date": "2026-03-26", "title": "Iran Rejects Direct US Talks; IRGC Opens Hormuz 'Tollbooth'", "type": "diplomatic", "description": "Iran formally rejects direct US negotiations, calling preconditions 'surrender terms.' IRGC confirms selective Hormuz transit for Chinese and Indian tankers paying transit fees — estimated $2-5M per passage. Western-flagged vessels still blocked. Brent rebounds to $108.01.", "severity": 4, "fatalities": 0, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-03-27", "title": "3,500 Marines Arrive on USS Tripoli; Humanitarian Ships Allowed", "type": "military", "description": "USS Tripoli arrives in Gulf of Oman with 3,500 Marines from 15th MEU. Pentagon announces humanitarian corridor through Hormuz for food and medical supplies. Iran allows passage of two humanitarian vessels as goodwill gesture while maintaining military blockade.", "severity": 4, "fatalities": 0, "lat": 25.0000, "lon": 57.5000, "location": "Gulf of Oman"},
        {"date": "2026-03-28", "title": "Houthis Fire Missiles at Israel; 82nd Airborne Deploys to Region", "type": "military", "description": "Houthis fire ballistic missiles at Israel for first time since war began — 11 injured from debris in southern Israel. Marks major Houthi escalation beyond Red Sea shipping attacks. 82nd Airborne Division deploys to region. Brent surges to $112.57. Renewed fears of Red Sea shipping strikes as Houthis re-enter direct combat.", "severity": 5, "fatalities": 0, "lat": 31.0461, "lon": 34.8516, "location": "Southern Israel"},
        {"date": "2026-03-29", "title": "Pentagon Prepares Ground Operations in Iran; 13 US KIA Total", "type": "military", "description": "Pentagon announces preparation for 'weeks of ground operations' targeting Qeshm Island and Kharg Island to permanently secure Hormuz and eliminate Iran's oil export capability. 82nd Airborne and Marine forces staging. Iran's IRGC threatens to 'rain fire' on any ground troops. US casualties now 13 KIA, 300+ wounded since Feb 28. Oil markets brace for further escalation.", "severity": 5, "fatalities": 0, "lat": 26.8500, "lon": 55.9000, "location": "Qeshm Island, Iran"},
    ]


def fetch_iran_news() -> List[dict]:
    """Fetch live Iran/oil war news headlines from Google News RSS. No API key needed."""
    cached = _read_cache("iran_news", 1800)  # 30-minute cache
    if cached:
        logger.info("Iran news: serving from cache")
        return cached

    try:
        import xml.etree.ElementTree as ET

        # Multiple targeted queries to capture different angles of the conflict
        queries = [
            "Iran+war+US+strikes",
            "Strait+of+Hormuz+oil+shipping",
            "Brent+crude+oil+Iran",
        ]
        seen_titles = set()
        all_articles = []

        def _fetch_rss(q):
            url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
            resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=3)
            if resp.status_code != 200:
                return []
            root = ET.fromstring(resp.text)
            items = []
            for item in root.findall(".//item"):
                title_el = item.find("title")
                pub_el = item.find("pubDate")
                source_el = item.find("source")
                link_el = item.find("link")
                if title_el is None or pub_el is None:
                    continue
                items.append({
                    "title": title_el.text or "",
                    "pubDate": pub_el.text or "",
                    "source": source_el.text if source_el is not None else "",
                    "url": link_el.text if link_el is not None else "",
                })
            return items

        # Fetch all RSS feeds in parallel
        with ThreadPoolExecutor(max_workers=3) as pool:
            rss_results = list(pool.map(_fetch_rss, queries))

        for items in rss_results:
            for a in items:
                title_lower = a["title"].lower()
                if title_lower not in seen_titles:
                    seen_titles.add(title_lower)
                    all_articles.append(a)

        # Relevance filter: must mention Iran/Hormuz AND oil/military/war context
        iran_terms = {"iran", "iranian", "tehran", "hormuz", "irgc", "persian gulf", "hezbollah", "houthi"}
        context_terms = {"oil", "brent", "crude", "war", "strike", "military", "missile",
                         "drone", "attack", "bomb", "navy", "sanctions", "nuclear",
                         "ceasefire", "tanker", "shipping", "casualt", "killed", "troops"}

        filtered = []
        for a in all_articles:
            t = a["title"].lower()
            has_iran = any(term in t for term in iran_terms)
            has_context = any(term in t for term in context_terms)
            if has_iran and has_context:
                filtered.append(a)

        # Classify type based on keywords
        def classify_type(title):
            t = title.lower()
            if any(w in t for w in ["oil", "brent", "crude", "price", "opec", "tanker", "shipping", "hormuz", "trade"]):
                return "proxy"
            if any(w in t for w in ["sanction", "embargo", "treasury", "ofac"]):
                return "sanctions"
            if any(w in t for w in ["nuclear", "uranium", "iaea", "enrich"]):
                return "nuclear"
            if any(w in t for w in ["talk", "negotiat", "diplomat", "ceasefire", "peace", "deal", "summit"]):
                return "diplomatic"
            return "military"

        # Parse dates and build structured output
        results = []
        for a in filtered[:50]:  # Cap at 50 most recent
            try:
                pub_dt = datetime.strptime(a["pubDate"][:25], "%a, %d %b %Y %H:%M:%S")
                date_str = pub_dt.strftime("%Y-%m-%d")
            except (ValueError, IndexError):
                continue

            results.append({
                "date": date_str,
                "title": a["title"],
                "type": classify_type(a["title"]),
                "source": a["source"],
                "url": a["url"],
                "severity": 3,  # Default; news headlines don't have severity
                "auto": True,   # Flag to distinguish from curated events
            })

        # Sort by date descending
        results.sort(key=lambda x: x["date"], reverse=True)

        if results:
            _write_cache("iran_news", results)
            logger.info(f"Iran news: fetched {len(results)} relevant articles from Google News")
        return results

    except Exception as e:
        logger.warning(f"Iran news fetch failed: {e}")
        return []


# ─── IMF PortWatch — Chokepoint Transit Data ─────────────────────────────────

_PORTWATCH_BASE = (
    "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/"
    "Daily_Chokepoints_Data/FeatureServer/0/query"
)

_CHOKEPOINT_IDS = {
    "suez": "chokepoint1",
    "bab_el_mandeb": "chokepoint4",
    "hormuz": "chokepoint6",
}


def _fetch_chokepoint_transits(chokepoint_key: str) -> List[dict]:
    """Fetch daily transit data from IMF PortWatch for a given chokepoint and aggregate to monthly totals.
    Returns list of {month, transits, tanker_transits, container_transits} sorted ascending."""

    cache_key = f"{chokepoint_key}_transits"
    cached = _read_cache(cache_key, ttl=86400)  # 24h cache — data updates weekly
    if cached:
        return cached

    port_id = _CHOKEPOINT_IDS.get(chokepoint_key)
    if not port_id:
        logger.error(f"Unknown chokepoint key: {chokepoint_key}")
        return []

    logger.info(f"Fetching {chokepoint_key} transit data from IMF PortWatch...")

    all_records = []
    offset = 0
    batch_size = 2000

    while True:
        params = {
            "where": f"portid='{port_id}' AND year>=2023 AND (year>2023 OR month>=7)",
            "outFields": "date,year,month,n_total,n_tanker,n_container,n_dry_bulk",
            "orderByFields": "date ASC",
            "resultRecordCount": batch_size,
            "resultOffset": offset,
            "f": "json",
        }
        try:
            resp = requests.get(_PORTWATCH_BASE, params=params, headers=_BROWSER_HEADERS, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning(f"PortWatch API request failed for {chokepoint_key} at offset {offset}: {e}")
            break

        features = data.get("features", [])
        if not features:
            break

        for f in features:
            all_records.append(f["attributes"])

        if len(features) < batch_size:
            break
        offset += batch_size

    if not all_records:
        logger.warning(f"No {chokepoint_key} transit data returned from PortWatch")
        return []

    # Aggregate daily records to monthly totals
    monthly = defaultdict(lambda: {"transits": 0, "tanker": 0, "container": 0, "dry_bulk": 0, "days": 0})
    for r in all_records:
        key = f"{r['year']}-{r['month']:02d}"
        monthly[key]["transits"] += r.get("n_total", 0) or 0
        monthly[key]["tanker"] += r.get("n_tanker", 0) or 0
        monthly[key]["container"] += r.get("n_container", 0) or 0
        monthly[key]["dry_bulk"] += r.get("n_dry_bulk", 0) or 0
        monthly[key]["days"] += 1

    # Build sorted result — only include months with at least 15 days of data
    result = []
    for month_key in sorted(monthly.keys()):
        m = monthly[month_key]
        if m["days"] < 15:
            continue
        result.append({
            "month": month_key,
            "transits": m["transits"],
            "tanker_transits": m["tanker"],
            "container_transits": m["container"],
            "dry_bulk_transits": m["dry_bulk"],
            "days_sampled": m["days"],
        })

    logger.info(f"{chokepoint_key} transit data: {len(result)} months from {len(all_records)} daily records")
    _write_cache(cache_key, result)
    return result


def fetch_suez_transits() -> List[dict]:
    """Fetch Suez Canal transit data."""
    return _fetch_chokepoint_transits("suez")


def fetch_bab_el_mandeb_transits() -> List[dict]:
    """Fetch Bab el-Mandeb Strait transit data."""
    return _fetch_chokepoint_transits("bab_el_mandeb")


def fetch_hormuz_transits() -> List[dict]:
    """Fetch Strait of Hormuz transit data."""
    return _fetch_chokepoint_transits("hormuz")


_HIGH_SIGNAL_WORDS = {"killed", "strike", "strikes", "missile", "attack", "destroyed", "explosion", "bomb", "drone", "fire", "burning", "casualties", "dead", "wounded"}
_ACLED_KEY_ACTORS = {"irgc", "united states", "israel", "hezbollah", "houthi", "navy", "air force", "centcom", "idf"}
_ACLED_TYPE_MAP = {
    "Battles": "military",
    "Explosions/Remote violence": "military",
    "Violence against civilians": "military",
    "Strategic developments": "diplomatic",
    "Protests": "proxy",
    "Riots": "proxy",
}

# Words that indicate a roundup/summary headline rather than a discrete event
_ROUNDUP_WORDS = {"live updates", "live:", "latest:", "what we know", "what happened",
                   "day of war", "here's what", "key developments", "live update",
                   "minute by minute", "as it happened", "rolling coverage"}

# High-credibility sources get a scoring boost
_TIER1_SOURCES = {"reuters", "associated press", "ap news", "bbc", "the new york times",
                  "the washington post", "the wall street journal", "al jazeera",
                  "the guardian", "cnn", "abc news", "nbc news", "financial times"}

# Words that indicate a major discrete event (strong signal)
_MAJOR_EVENT_WORDS = {
    "seize": 3, "seized": 3, "sinks": 4, "sunk": 4, "torpedoed": 4,
    "invades": 4, "invasion": 4, "ceasefire": 4, "surrenders": 4,
    "assassinated": 4, "killed": 3, "destroys": 3, "destroyed": 3,
    "launches": 2, "fires": 2, "strikes": 2, "hits": 2, "shoots": 2,
    "intercepts": 2, "blocks": 2, "closes": 3, "shuts": 2,
    "deploys": 2, "mobilizes": 2, "evacuates": 2,
    "sanctions": 2, "ultimatum": 3, "declares": 2, "threatens": 1,
    "surges": 2, "crashes": 2, "spikes": 2, "plunges": 2,
    "mined": 3, "mines": 2, "torpedo": 3, "boarding": 2,
    "shoots down": 3, "shot down": 3,
}


def _score_news_headline(item: dict) -> int:
    """Score a news headline for significance. Higher = more important."""
    title = item.get("title", "")
    title_lower = title.lower()
    source = item.get("source", "").lower()
    score = 0

    # Reject roundup/summary headlines — these aren't discrete events
    if any(rw in title_lower for rw in _ROUNDUP_WORDS):
        return -1

    # Major event keyword scoring
    for word, pts in _MAJOR_EVENT_WORDS.items():
        if word in title_lower:
            score += pts

    # High-signal words (general)
    words = set(title_lower.split())
    score += len(words & _HIGH_SIGNAL_WORDS) * 2

    # Named key actors
    for actor in _ACLED_KEY_ACTORS:
        if actor in title_lower:
            score += 1

    # Specificity bonus: numbers in headline (casualties, counts, distances)
    if re.search(r'\b\d+\b', title):
        score += 1

    # Source credibility boost
    if any(s in source for s in _TIER1_SOURCES):
        score += 2

    # Location specificity bonus (not just "Iran" — a specific place)
    specific_locations = {"hormuz", "natanz", "isfahan", "fordow", "bushehr", "bandar abbas",
                          "tehran", "qeshm", "fujairah", "ras tanura", "salalah", "jask"}
    if any(loc in title_lower for loc in specific_locations):
        score += 2

    return score


def _geocode_news_events(news_items: List[dict]) -> List[dict]:
    """Convert significant Google News headlines into curated-quality timeline events.

    Scores headlines for significance and promotes only the top 1-2 per day,
    filtering out roundup articles and low-signal noise.
    """
    # Score and geocode all candidates
    candidates = []
    for item in news_items:
        title = item.get("title", "")
        title_lower = title.lower()

        score = _score_news_headline(item)
        if score < 4:  # Minimum threshold — must be clearly significant
            continue

        # Find location by longest-first keyword match
        lat, lon, location = None, None, None
        for key in _LOCATION_KEYS_SORTED:
            if key in title_lower:
                lat, lon, location = CONFLICT_THEATER_LOCATIONS[key]
                break

        if lat is None:
            continue  # Can't plot without coordinates

        severity = 5 if score >= 10 else (4 if score >= 6 else 3)

        candidates.append({
            "date": item.get("date", "")[:10],
            "title": title,
            "type": item.get("type", "military"),
            "description": f"Live news via {item.get('source', 'Unknown')}",
            "severity": severity,
            "lat": lat,
            "lon": lon,
            "location": location,
            "fatalities": 0,
            "source_type": "news_auto",
            "_score": score,
        })

    # Keep only top 2 per date (highest scoring)
    by_date = defaultdict(list)
    for c in candidates:
        by_date[c["date"]].append(c)

    results = []
    for date, events in by_date.items():
        events.sort(key=lambda x: x["_score"], reverse=True)
        for ev in events[:2]:
            del ev["_score"]
            results.append(ev)

    logger.info(f"News scoring: {len(news_items)} articles → {len(candidates)} significant → {len(results)} promoted")
    return results


def _promote_acled_events(acled_events: List[dict], curated_events: List[dict]) -> List[dict]:
    """Score and promote the most significant ACLED events to curated quality."""
    # Build curated index for deduplication: (date, lat, lon)
    curated_index = []
    for c in curated_events:
        try:
            curated_index.append((c["date"], float(c.get("lat", 0)), float(c.get("lon", 0))))
        except (ValueError, TypeError):
            pass

    def _is_near_curated(date_str, lat, lon):
        for c_date, c_lat, c_lon in curated_index:
            if abs(ord(date_str[8]) - ord(c_date[8])) <= 1 and date_str[:7] == c_date[:7]:
                if abs(lat - c_lat) < 0.5 and abs(lon - c_lon) < 0.5:
                    return True
        return False

    scored = []
    for e in acled_events:
        try:
            lat = float(e.get("latitude", 0))
            lon = float(e.get("longitude", 0))
        except (ValueError, TypeError):
            continue
        if not lat or not lon:
            continue

        fatalities = int(e.get("fatalities", 0) or 0)
        event_type = e.get("event_type", "")
        sub_type = e.get("sub_event_type", "")
        actor1 = (e.get("actor1", "") or "").lower()
        actor2 = (e.get("actor2", "") or "").lower()
        location = e.get("location", "")
        date_str = (e.get("event_date", "") or "")[:10]

        if not date_str or len(date_str) < 10:
            continue

        # Skip if near an existing curated event
        if _is_near_curated(date_str, lat, lon):
            continue

        # Skip domestic protests/riots — not war-relevant
        if event_type in ("Protests", "Riots"):
            continue

        # Score
        score = 0
        score += min(30, fatalities * 3)
        if event_type in ("Explosions/Remote violence", "Battles"):
            score += 5
        if any(k in actor1 or k in actor2 for k in _ACLED_KEY_ACTORS):
            score += 5
        loc_lower = location.lower()
        if any(k in loc_lower for k in _LOCATION_KEYS_SORTED[:20]):
            score += 3
        if any(k in sub_type.lower() for k in ("air/drone strike", "shelling", "artillery", "armed clash", "suicide bomb")):
            score += 2

        if score < 8:
            continue

        # Generate title
        if sub_type and location:
            title = f"{sub_type} in {location}"
        elif actor1 and location:
            actor_display = actor1.split("(")[0].strip().title()
            title = f"{event_type}: {actor_display} in {location}"
        else:
            title = f"{event_type} in {location or 'Iran'}"

        # Map severity
        if score >= 19:
            severity = 5
        elif score >= 13:
            severity = 4
        else:
            severity = 3

        scored.append({
            "date": date_str,
            "title": title,
            "type": _ACLED_TYPE_MAP.get(event_type, "military"),
            "description": (e.get("notes", "") or "")[:500],
            "severity": severity,
            "lat": lat,
            "lon": lon,
            "location": location,
            "fatalities": fatalities,
            "source_type": "acled_promoted",
            "_score": score,
        })

    # Sort by score, take top 20
    scored.sort(key=lambda x: x["_score"], reverse=True)
    for item in scored:
        del item["_score"]
    return scored[:20]


def get_merged_curated_events(include_news: bool = True) -> List[dict]:
    """Merge hardcoded curated events with auto-discovered OSINT events.

    Args:
        include_news: If True, geocode live news headlines and merge.
                      Set False when called from compute_iran_impact() to avoid
                      blocking on Google News fetches.
    """
    # 1. Hardcoded curated events
    curated = get_curated_iran_events()
    for e in curated:
        if "source_type" not in e:
            e["source_type"] = "curated"

    news_events = []

    # 2. Geocoded news events (only if requested and cache is warm)
    if include_news:
        try:
            # Only use cached news — never trigger a live fetch here
            cached_news = _read_cache("iran_news", 1800)
            if cached_news:
                news_events = _geocode_news_events(cached_news)
                # Deduplicate news against curated (same date + nearby location)
                curated_index = [(c["date"], c.get("lat", 0), c.get("lon", 0)) for c in curated]
                deduped_news = []
                for ne in news_events:
                    is_dup = False
                    for c_date, c_lat, c_lon in curated_index:
                        if ne["date"] == c_date and abs(ne["lat"] - c_lat) < 0.5 and abs(ne["lon"] - c_lon) < 0.5:
                            is_dup = True
                            break
                    if not is_dup:
                        deduped_news.append(ne)
                news_events = deduped_news
        except Exception as e:
            logger.warning(f"News geocoding failed: {e}")
            news_events = []

    # 3. Merge and sort (curated + news only; ACLED-promoted are internal Iranian
    #    conflicts like armed clashes/IEDs, not US-Iran tensions — excluded)
    merged = curated + news_events
    merged.sort(key=lambda x: x.get("date", ""), reverse=True)

    logger.info(f"Merged events: {len(curated)} curated + {len(news_events)} news = {len(merged)} total")
    return merged


def compute_iran_impact(iran_events: list, brent_prices: list) -> dict:
    """Calculate oil price impact metrics around Iran events."""
    if not brent_prices:
        return {"kpis": {}, "impact_by_type": {}, "event_table": []}

    # Build price lookup by date
    price_map = {p["date"]: p["price"] for p in brent_prices}
    sorted_dates = sorted(price_map.keys())

    def get_closing_price_before(date_str: str):
        """Get the closing price on the most recent trading day BEFORE this date."""
        idx = bisect.bisect_left(sorted_dates, date_str)
        if idx > 0:
            return price_map[sorted_dates[idx - 1]]
        return None

    def get_closing_price_after(date_str: str, days: int):
        """Get the closing price on the nearest trading day on or after date + days."""
        target = (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=days)).strftime("%Y-%m-%d")
        idx = bisect.bisect_left(sorted_dates, target)
        if idx < len(sorted_dates):
            return price_map[sorted_dates[idx]]
        # If past the end, use the last available price
        if sorted_dates:
            return price_map[sorted_dates[-1]]
        return None

    # Get curated events for impact table (skip news fetch — avoid blocking)
    curated = get_merged_curated_events(include_news=False)

    # Group curated events by TRADING day.
    # Weekend events (Sat/Sun) roll forward to the next Monday when markets
    # actually reacted, so before/after prices form a clean chain.

    def _next_trading_day(date_str: str) -> str:
        """If date falls on a weekend, roll forward to Monday."""
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        while dt.weekday() >= 5:  # 5=Sat, 6=Sun
            dt += timedelta(days=1)
        return dt.strftime("%Y-%m-%d")

    events_by_trading_day = OrderedDict()
    for ev in curated:
        td = _next_trading_day(ev["date"])
        if td not in events_by_trading_day:
            events_by_trading_day[td] = []
        events_by_trading_day[td].append(ev)

    # Build impact table — one row per trading day
    event_table = []
    for td, day_events in events_by_trading_day.items():
        price_before = get_closing_price_before(td)
        price_after = get_closing_price_after(td, 0)  # Trading-day close

        change_pct = None
        if price_before and price_after:
            change_pct = round((price_after - price_before) / price_before * 100, 2)

        # Collect all event details for this trading day
        max_severity = max(ev["severity"] for ev in day_events)
        types = list(dict.fromkeys(ev["type"] for ev in day_events))  # unique, order-preserving

        # Show the actual date range if events span multiple calendar days
        event_dates = sorted(set(ev["date"] for ev in day_events))
        display_date = event_dates[0] if len(event_dates) == 1 else f"{event_dates[0]} – {event_dates[-1]}"

        event_table.append({
            "date": td,                        # trading day (for sorting/filtering)
            "display_date": display_date,       # shows date range if weekend events rolled in
            "events": [{"title": ev["title"], "type": ev["type"], "severity": ev["severity"],
                         "description": ev.get("description", ""),
                         "actual_date": ev["date"]} for ev in day_events],
            "title": day_events[0]["title"],    # primary event title (backwards compat)
            "type": types[0],                   # primary type (backwards compat)
            "types": types,                     # all types for the day
            "severity": max_severity,
            "brent_before": round(price_before, 2) if price_before else None,
            "brent_after": round(price_after, 2) if price_after else None,
            "change_pct": change_pct,
        })

    # Aggregate impact by event type at different horizons
    curated_by_type = {}
    for ev in curated:
        t = ev["type"]
        if t not in curated_by_type:
            curated_by_type[t] = []
        curated_by_type[t].append(ev["date"])

    impact_by_type = {}
    for etype, dates in curated_by_type.items():
        offsets = {"T+1": 1, "T+3": 3, "T+5": 5, "T+7": 7}
        type_impact = {}
        for label, off in offsets.items():
            changes = []
            for d in dates:
                pb = get_closing_price_before(d)
                pa = get_closing_price_after(d, off)
                if pb and pa:
                    changes.append((pa - pb) / pb * 100)
            type_impact[label] = round(sum(changes) / len(changes), 3) if changes else 0
        impact_by_type[etype] = type_impact

    # ACLED-based aggregation
    acled_dates = list({e.get("event_date", "")[:10] for e in iran_events if e.get("event_date")})
    all_changes_3d = []
    max_vol_spike = 0
    current_month = datetime.now().strftime("%Y-%m")
    acled_this_month = sum(1 for e in iran_events if (e.get("event_date") or "").startswith(current_month))
    curated_this_month = sum(1 for e in curated if e["date"].startswith(current_month))
    events_this_month = acled_this_month + curated_this_month

    for d in acled_dates:
        pb = get_closing_price_before(d)
        pa = get_closing_price_after(d, 3)
        if pb and pa:
            change = abs(pa - pb)
            all_changes_3d.append(change)
            if change > max_vol_spike:
                max_vol_spike = change

    kpis = {
        "total_events": len(iran_events),
        "avg_price_move_3d": round(sum(all_changes_3d) / len(all_changes_3d), 2) if all_changes_3d else 0,
        "peak_volatility_spike": round(max_vol_spike, 2),
        "events_this_month": events_this_month,
    }

    return {
        "kpis": kpis,
        "impact_by_type": impact_by_type,
        "event_table": event_table,
    }


def get_hypothesis_results() -> dict:
    """Return hypothesis test results from the thesis defense regression table.
    Source: ThesisDefense_Hamm.pptx Slide 24 — OLS with HAC standard errors,
    7 control variables (DXY, OVX, SPR, OPEC Dummy, Russia-Ukraine Dummy,
    China PMI, Baker Hughes Rigs). 505 observations.
    """
    return {
        "h1": {
            "name": "H1: Attack Frequency",
            "description": "Higher frequency of Houthi maritime attacks increases Brent crude oil price volatility",
            "coefficient": -0.033,
            "p_value": 0.026,
            "r_squared": 0.487,
            "supported": False,
            "conclusion": "NOT SUPPORTED. The coefficient is statistically significant but negative (β = −0.033, p = 0.026), meaning more attacks are associated with less volatility — the opposite of the fear premium hypothesis. This supports market adaptation: as attacks became routine, markets priced in the disruption rather than reacting with increased volatility."
        },
        "h2": {
            "name": "H2: Tanker Specificity",
            "description": "Attacks specifically targeting oil tankers have a greater impact on volatility than general maritime attacks",
            "coefficient": -0.233,
            "p_value": 0.004,
            "r_squared": 0.464,
            "supported": False,
            "conclusion": "NOT SUPPORTED. Tanker-specific attacks show the strongest adaptation effect (β = −0.233, p = 0.004). Despite targeting energy infrastructure directly, tanker attacks are associated with reduced volatility — markets specifically recalibrated to energy-sector targeting."
        },
        "h3": {
            "name": "H3: Chokepoint Geography",
            "description": "Attacks at the Bab el-Mandeb strait chokepoint have a disproportionate impact on oil price volatility",
            "coefficient": -0.128,
            "p_value": 0.250,
            "r_squared": 0.459,
            "supported": False,
            "conclusion": "NOT SUPPORTED. The chokepoint coefficient is not statistically significant (β = −0.128, p = 0.250). Despite 67% of attacks concentrating in a 200km corridor near Bab el-Mandeb, geographic proximity alone does not predict oil price volatility."
        },
        "garch_summary": {
            "model": "GJR-GARCH(1,1,1)",
            "distribution": "Normal",
            "mean_model": "Constant",
            "observations": 505,
            "log_likelihood": -1027.56,
            "aic": 2065,
            "bic": 2086,
        },
        "model_comparison": {
            "labels": ["H1 (Attack Freq)", "H2 (Tanker)", "H3 (Chokepoint)"],
            "r_squared": [0.487, 0.464, 0.459],
            "finding": "With all 7 control variables and GARCH conditional variance as the dependent variable, models explain approximately 46–49% of volatility variance. H1 (p = 0.026) and H2 (p = 0.004) are statistically significant but negative — supporting market adaptation rather than a fear premium. H3 (p = 0.250) is not significant. Macroeconomic controls (OVX, DXY, Baker Hughes) dominate explanatory power."
        }
    }
