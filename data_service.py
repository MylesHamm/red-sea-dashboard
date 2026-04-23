"""
Data Service Layer - API integration with caching and CSV fallback.
Handles: ACLED, EIA (Brent + SPR), yfinance (DXY, OVX), FRED (China BCI)
"""
import bisect
import json
import math
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
_acled_fetch_error = None  # Store last ACLED (main) fetch error for debugging
_acled_fetch_source = None  # 'api' | 'cache' | 'fallback' | None
_acled_fetch_ts = 0.0


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
        try:
            token_data = resp.json()
        except Exception as e:
            raise requests.HTTPError(f"ACLED token response not JSON: {resp.text[:300]}") from e

        if "access_token" not in token_data:
            # Log the response so we can see what ACLED returned
            err_msg = token_data.get("error_description") or token_data.get("error") or str(token_data)[:300]
            raise requests.HTTPError(f"ACLED token response missing access_token: {err_msg}")

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
    df = df.rename(columns=col_map).copy()
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
    """Fetch paginated ACLED results.

    Timeout: 30s. ACLED's multi-year paginated queries (limit=5000) routinely
    take 5-15s per page, and Render's free-tier egress is slower than a dev
    laptop, so a 10s timeout silently fell through to the frozen fallback on
    every live fetch. 30s leaves headroom without pinning threads indefinitely.
    """
    results = []
    for page in range(1, max_pages + 1):
        p = {**params, "page": page}
        resp = requests.get(
            config.ACLED_DATA_URL,
            headers={**_BROWSER_HEADERS, "Authorization": f"Bearer {token}"},
            params=p,
            timeout=30,
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


_acled_events_memo: Optional[List[dict]] = None
_acled_events_memo_ts: float = 0.0
_acled_events_memo_lock = threading.Lock()

def fetch_acled_events() -> List[dict]:
    """Fetch comprehensive Houthi/Red Sea events from ACLED with multi-query approach.

    Strategy:
    1. All Yemen events (primary conflict zone)
    2. Houthi/Ansar Allah actor events globally (maritime attacks outside Yemen)
    3. Red Sea regional countries filtered for maritime relevance

    In-process memoization: the on-disk cache is ~13MB of JSON which
    materializes to 40-70MB of Python dicts per parse. Without the in-process
    cache, every /api/events request re-parses that blob (and on Render's
    512MB free tier two concurrent parses are enough to OOM the worker).
    """
    global _acled_events_memo, _acled_events_memo_ts
    global _acled_fetch_error, _acled_fetch_source, _acled_fetch_ts
    # Fast path: serve from in-process memo (re-check after acquiring the lock
    # in case another thread populated it while we were waiting).
    if _acled_events_memo is not None and time.time() - _acled_events_memo_ts < 600:
        return _acled_events_memo

    cached = _read_cache("acled_events", config.CACHE_TTL_ACLED)
    if cached:
        with _acled_events_memo_lock:
            _acled_events_memo = cached
            _acled_events_memo_ts = time.time()
        _acled_fetch_source = "cache"
        _acled_fetch_ts = time.time()
        _acled_fetch_error = None
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

        # Cap concurrency at 3 so we only hold 3 in-flight response bodies in
        # memory at a time (previously 6 => spikes of ~80MB on Render free).
        with ThreadPoolExecutor(max_workers=3) as pool:
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
            with _acled_events_memo_lock:
                _acled_events_memo = all_events
                _acled_events_memo_ts = time.time()
            _acled_fetch_source = "api"
            _acled_fetch_ts = time.time()
            _acled_fetch_error = None
            logger.info(f"ACLED: total {len(all_events)} unique events fetched and cached")
            return all_events
        else:
            _acled_fetch_error = "ACLED API returned 0 events for every query"
            logger.warning(_acled_fetch_error)

    except Exception as e:
        _acled_fetch_error = f"{type(e).__name__}: {e}"
        logger.warning(f"ACLED API failed: {_acled_fetch_error}")

    fallback = _load_acled_fallback()
    # Memoize the fallback too — it's the same 13MB blob
    if fallback:
        with _acled_events_memo_lock:
            _acled_events_memo = fallback
            _acled_events_memo_ts = time.time()
        _acled_fetch_source = "fallback"
        _acled_fetch_ts = time.time()
    return fallback


def get_acled_fetch_error() -> Optional[str]:
    return _acled_fetch_error


def get_acled_fetch_meta() -> dict:
    """Diagnostic metadata about the most recent ACLED fetch."""
    return {
        "source": _acled_fetch_source,
        "ts": _acled_fetch_ts,
        "error": _acled_fetch_error,
        "credentials_configured": bool(config.ACLED_USERNAME and config.ACLED_PASSWORD),
    }


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
        df = pd.read_csv(csv_path).copy()
        col_map = {c: c.lower() for c in df.columns}
        df = df.rename(columns=col_map)

        # Map CH6 column names to expected schema
        if "date" in df.columns and "event_date" not in df.columns:
            df = df.rename(columns={"date": "event_date"})
        if "event_id" in df.columns and "event_id_cnty" not in df.columns:
            df = df.rename(columns={"event_id": "event_id_cnty"})

        records = _df_to_event_records(df.copy())
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
        # Mar 29 (Sat) and Mar 30 (Sun) — no settlement
        "2026-03-31": 115.24,  # Continued airstrikes; parliament rejects negotiations
        "2026-04-01": 113.88,  # UK 35-nation Hormuz meeting; diplomatic push
        "2026-04-02": 111.45,  # Trump says objectives nearly met; coalition diplomacy
        "2026-04-03": 118.32,  # US F-15E shot down over Iran; escalation fears
        "2026-04-04": 121.07,  # Bushehr nuclear plant struck; IAEA warning
        # Apr 5 (Sat) and Apr 6 (Sun) — no settlement
        "2026-04-07": 126.00,  # Trump 'civilization will die tonight'; ceasefire announced
        "2026-04-08": 93.76,   # Ceasefire crash — Brent drops ~13% from highs
        "2026-04-09": 100.99,  # Rebound as Hormuz stays closed despite ceasefire
        "2026-04-10": 97.78,   # Trump accuses Iran of poor Hormuz management
        "2026-04-11": 95.71,   # US Navy enters Hormuz; Islamabad talks begin
        # Apr 12 (Sat) — no settlement
        "2026-04-13": 103.72,  # Surges 6.95% on US naval blockade announcement
        "2026-04-14": 100.19,  # Blockade takes effect; Trump hints at resumed talks
        "2026-04-15": 96.83,   # Diplomatic optimism — "very close to over"
        "2026-04-16": 94.89,   # Lebanon 10-day truce announced; regional tensions ease
        "2026-04-17": 93.50,   # Hormuz declared open during ceasefire — relief continues
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
    """Load the master dataset CSV and return as structured JSON.

    Fast path: if data/master_dataset.json (pre-computed at build / committed
    to repo) exists, serve it directly. This avoids cold-start pandas work on
    Render's free tier where the live computation can exceed Cloudflare's
    edge timeout, leaving the cache permanently un-warmed. The static file
    only needs to be regenerated when the underlying CSV changes — live
    Brent / DXY / OVX KPIs are still pulled from their dedicated cached
    endpoints by the frontend.
    """
    static_json = config.DATA_DIR / "master_dataset.json"
    if static_json.exists():
        try:
            with open(static_json, "r") as f:
                result = json.load(f)
            # Refresh the live KPI fields from caches if available so the
            # static file doesn't go stale on price snapshots.
            try:
                live_brent = _read_cache("brent_prices", config.CACHE_TTL_BRENT)
                if live_brent and len(live_brent) >= 2:
                    result.setdefault("kpis", {})
                    result["kpis"]["latest_brent_price"] = round(live_brent[-1]["price"], 2)
                    result["kpis"]["brent_price_change"] = round(live_brent[-1]["price"] - live_brent[-2]["price"], 2)
                live_dxy = _read_cache("dxy", config.CACHE_TTL_YFINANCE)
                if live_dxy:
                    result.setdefault("kpis", {})["latest_dxy"] = round(live_dxy[-1]["value"], 2)
                live_ovx = _read_cache("ovx", config.CACHE_TTL_YFINANCE)
                if live_ovx:
                    result.setdefault("kpis", {})["latest_ovx"] = round(live_ovx[-1]["value"], 2)
            except Exception as e:
                logger.warning(f"master_dataset.json: could not refresh live KPIs: {e}")
            return result
        except Exception as e:
            logger.error(f"master_dataset.json read failed, falling back to live: {e}")

    cached = _read_cache("master_dataset", 1800)  # 30 min cache
    if cached:
        return cached

    if not config.MYLES_DATASET_PATH.exists():
        logger.error(f"Master dataset not found: {config.MYLES_DATASET_PATH}")
        return {"timeseries": [], "kpis": {}, "price_windows": {}, "correlation": []}

    df = pd.read_csv(config.MYLES_DATASET_PATH).copy()
    date_col = df.columns[0]
    df = df.rename(columns={date_col: "Date"}).copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date").copy()

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
                    timeout=30,
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
                timeout=30,
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


_iran_fallback_memo: Optional[List[dict]] = None

def _load_iran_json_fallback() -> List[dict]:
    """Load Iran events from local JSON fallback file.

    Memoized after first successful parse — the 400KB file materializes to
    ~2MB of dicts and was previously being reparsed on every /api/iran-events
    and /api/iran-impact call.
    """
    global _iran_fallback_memo
    if _iran_fallback_memo is not None:
        return _iran_fallback_memo
    path = config.DATA_DIR / "iran_events.json"
    if not path.exists():
        logger.warning("Iran events: no JSON fallback file found")
        return []
    try:
        events = json.loads(path.read_text())
        _iran_fallback_memo = events
        logger.info(f"Iran events: loaded {len(events)} events from JSON fallback")
        return events
    except Exception as e:
        logger.warning(f"Iran events JSON fallback failed: {e}")
        return []


def get_iran_fetch_error() -> Optional[str]:
    return _iran_fetch_error


def get_curated_iran_events() -> List[dict]:
    """Return curated timeline of major US-Iran events (2025-2026) with coordinates.

    Severity scale (1-5):
        5 = Civilization-altering (nuclear/WMD use, total global energy shutdown)
        4 = Major war escalation (war begins, Hormuz closed, massive retaliation, 100+ killed/day)
        3 = Significant military action (major strikes, infrastructure hit, new fronts open)
        2 = Provocations & buildups (tanker seizures, force deployments, sanctions)
        1 = Diplomatic/rhetorical (talks, threats, policy shifts)
    """
    return [
        # ── Phase 1: Maximum Pressure Restored (Jan-Feb 2025) ──
        {"date": "2025-01-20", "title": "Trump Inaugurated, Rescinds Biden-Era Iran Policies", "type": "diplomatic", "description": "Trump signs EO 14148 rescinding 67 Biden-era executive orders including Iran sanctions-related actions.", "severity": 1, "lat": 38.9072, "lon": -77.0369, "location": "Washington, DC"},
        {"date": "2025-02-04", "title": "Trump Signs 'Maximum Pressure' Executive Order", "type": "sanctions", "description": "NSPM-2 restores maximum pressure campaign: Treasury to impose maximum economic pressure, State Dept rescinds sanctions waivers, campaign to drive Iran oil exports to zero.", "severity": 2, "lat": 38.9072, "lon": -77.0369, "location": "Washington, DC"},
        {"date": "2025-02-06", "title": "OFAC Sanctions Iranian Oil Shipping Network", "type": "sanctions", "description": "Treasury's OFAC sanctions international network of parties and vessels facilitating Iranian crude oil shipments to China.", "severity": 1, "lat": 38.9072, "lon": -77.0369, "location": "Washington, DC"},

        # ── Phase 2: Nuclear Talks Begin (Mar-Jun 2025) ──
        {"date": "2025-03-07", "title": "Trump Sends Letter to Khamenei with 2-Month Deadline", "type": "diplomatic", "description": "Trump sends letter via UAE diplomat Anwar Gargash proposing nuclear negotiations, warning of military consequences if rejected.", "severity": 1, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2025-04-12", "title": "Round 1: US-Iran Indirect Talks Begin in Muscat", "type": "diplomatic", "description": "First indirect US-Iran nuclear talks mediated by Oman. US envoy Witkoff and Iranian FM Araghchi in separate rooms. Both sides call talks 'constructive.'", "severity": 1, "lat": 23.5880, "lon": 58.3829, "location": "Muscat, Oman"},
        {"date": "2025-05-11", "title": "Round 4: US Demands Complete Dismantlement", "type": "diplomatic", "description": "Fourth round in Muscat. Witkoff demands complete dismantlement of Natanz, Fordow, and Isfahan. Positions harden.", "severity": 1, "lat": 23.5880, "lon": 58.3829, "location": "Muscat, Oman"},
        {"date": "2025-05-31", "title": "IAEA: Iran Has 400+ kg of 60% Enriched Uranium", "type": "nuclear", "description": "Confidential IAEA report confirms 400+ kg of 60% enriched uranium, enough for ~10 nuclear weapons if further enriched; total stockpile 40x JCPOA limit.", "severity": 3, "lat": 33.5103, "lon": 51.9250, "location": "Natanz, Iran"},

        # ── Phase 3: The Twelve-Day War (Jun 2025) ──
        {"date": "2025-06-13", "title": "Israel Launches 'Operation Rising Lion': Strikes Iran", "type": "military", "description": "Israel launches surprise strikes on Iranian nuclear facilities including Natanz. Prominent military leaders and nuclear scientists assassinated. US-Iran talks suspended.", "severity": 4, "lat": 33.5103, "lon": 51.9250, "location": "Natanz, Iran"},
        {"date": "2025-06-21", "title": "US Launches 'Operation Midnight Hammer'", "type": "military", "description": "125+ aircraft including seven B-2 bombers with GBU-57 bunker busters strike Fordow, Natanz, Isfahan. Tomahawks from submarines. Trump claims facilities 'obliterated.'", "severity": 4, "lat": 34.7564, "lon": 51.0596, "location": "Fordow, Iran"},
        {"date": "2025-06-22", "title": "Iran Retaliates: 550+ Missiles, 1000+ Drones at Israel", "type": "military", "description": "Iran launches over 550 ballistic missiles and 1,000+ drones at Israeli and US targets. Most intercepted by Israel and US.", "severity": 4, "lat": 32.0853, "lon": 34.7818, "location": "Tel Aviv, Israel"},
        {"date": "2025-06-24", "title": "Twelve-Day War Ceasefire Agreed", "type": "diplomatic", "description": "Israel and Iran agree to ceasefire under US pressure, ending the Twelve-Day War.", "severity": 1, "lat": 38.9072, "lon": -77.0369, "location": "Washington, DC"},

        # ── Phase 4: Snapback Sanctions & Isolation (Aug-Oct 2025) ──
        {"date": "2025-08-28", "title": "E3 Triggers JCPOA Snapback Sanctions Mechanism", "type": "sanctions", "description": "UK, France, and Germany invoke JCPOA snapback citing Iran's 'significant non-performance.' 30-day countdown begins.", "severity": 2, "lat": 40.7489, "lon": -73.9680, "location": "New York, NY (UN)"},
        {"date": "2025-09-27", "title": "UN Snapback Sanctions Formally Reimposed on Iran", "type": "sanctions", "description": "All UN sanctions lifted under JCPOA formally reimposed: travel bans, asset freezes, arms embargo, ballistic missile restrictions. EU follows Sept 29.", "severity": 2, "lat": 40.7489, "lon": -73.9680, "location": "New York, NY (UN)"},
        {"date": "2025-10-18", "title": "Iran Officially Terminates the JCPOA", "type": "diplomatic", "description": "Iran declares the JCPOA over on 'Termination Day.' Iran, Russia, and China declare UN sanctions invalid.", "severity": 2, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},

        # ── Phase 5: Protests & Crackdown (Dec 2025 - Jan 2026) ──
        {"date": "2025-12-28", "title": "Massive Anti-Regime Protests Erupt Across Iran", "type": "proxy", "description": "Protests erupt after rial collapses to 1.4M/$1. Tehran Grand Bazaar strikes spread nationwide. 72% food inflation, post-war devastation, snapback sanctions.", "severity": 2, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-01-08", "title": "Iran's Deadliest Crackdown Since 1979", "type": "military", "description": "Security forces launch massive crackdown. Internet fully cut. Firearms and shotguns with metal pellets used against protesters. Thousands reported killed.", "severity": 3, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-01-23", "title": "Trump Announces Naval 'Armada' Heading to Middle East", "type": "military", "description": "Trump announces USS Abraham Lincoln carrier strike group deployment. F/A-18E Super Hornets, F-35C Lightning IIs, guided-missile destroyers.", "severity": 2, "lat": 25.2854, "lon": 55.3500, "location": "Persian Gulf"},

        # ── Phase 6: War Buildup & Hormuz Provocations (Feb 2026) ──
        {"date": "2026-01-30", "title": "IRGC Seizes South Korean Tanker in Strait of Hormuz", "type": "military", "description": "IRGC Navy commandos fast-rope onto South Korean chemical tanker 'Hankuk Chemi II' in Strait of Hormuz, citing 'environmental violations.' Crew of 20 detained at Bandar Abbas. Seoul condemns 'act of piracy.' Mirrors 2021 tanker seizure. Oil markets spike 3.2%.", "severity": 2, "fatalities": 0, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-02-03", "title": "IRGC Attempts to Board US Tanker; Drone Shot Down", "type": "military", "description": "IRGC Navy attempts to intercept US-flagged tanker in Strait of Hormuz. USS McFaul escorts it to safety. F-35C shoots down Iranian Shahed-136 drone.", "severity": 2, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-02-10", "title": "IRGC Fires Warning Shots at Norwegian Tanker Near Hormuz", "type": "military", "description": "IRGC Navy patrol boat fires warning shots across bow of Norwegian-flagged tanker 'Nordic Spirit' in international waters near Hormuz. USS Carney intervenes; IRGC boats withdraw. Norway summons Iranian ambassador. Third Hormuz shipping incident in 11 days.", "severity": 2, "fatalities": 0, "lat": 26.4800, "lon": 56.3000, "location": "Strait of Hormuz"},
        {"date": "2026-02-06", "title": "Round 6: US-Iran Talks Resume in Muscat", "type": "diplomatic", "description": "First talks since June 2025. US delegation: Witkoff, Kushner, CENTCOM commander Adm. Cooper. Iranian FM Araghchi leads. 'Good start.'", "severity": 1, "lat": 23.5880, "lon": 58.3829, "location": "Muscat, Oman"},
        {"date": "2026-02-13", "title": "USS Gerald R. Ford Redeployed; Trump Signals Regime Change", "type": "military", "description": "Ford redirected to Middle East, largest US force posture since 2003 Iraq War. Trump says regime change would be 'best thing that could happen.'", "severity": 2, "lat": 25.2854, "lon": 55.3500, "location": "Persian Gulf"},
        {"date": "2026-02-14", "title": "Pentagon Prepares 'Weeks-Long Sustained Operations'", "type": "military", "description": "US officials confirm military is preparing for sustained operations against Iran lasting weeks.", "severity": 2, "lat": 38.8719, "lon": -77.0563, "location": "Pentagon, VA"},
        {"date": "2026-02-19", "title": "Trump Gives Iran 10-Day Ultimatum", "type": "diplomatic", "description": "Trump tells Iran to reach a 'meaningful' deal within 10-15 days or 'really bad things' will happen. IRGC conducts live-fire Strait of Hormuz drill.", "severity": 2, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-02-24", "title": "F-22s Deployed to Israel; State of the Union Warning", "type": "military", "description": "12 F-22s deployed to Ovda Airbase, the first US offensive weapons in Israel. Trump vows in SOTU that Iran will never have nuclear weapons.", "severity": 2, "lat": 29.9402, "lon": 34.9358, "location": "Ovda Airbase, Israel"},
        {"date": "2026-02-26", "title": "Final Nuclear Talks Fail: No Deal Reached", "type": "diplomatic", "description": "Round 8 in Geneva. US demands: destroy all enrichment sites, surrender uranium, permanent deal, end proxies. Iran refuses missile restrictions. No agreement.", "severity": 2, "lat": 46.2044, "lon": 6.1432, "location": "Geneva, Switzerland"},
        {"date": "2026-02-27", "title": "IAEA Reveals Hidden Uranium; Embassies Evacuate Iran", "type": "nuclear", "description": "IAEA reports 440.9 kg of 60% enriched uranium hidden in Isfahan tunnels. Embassies evacuate. Trump gives go order for Operation Epic Fury from Air Force One.", "severity": 3, "lat": 32.6546, "lon": 51.6680, "location": "Isfahan, Iran"},

        # ── Phase 7: Operation Epic Fury / Iran War (Feb 28+, 2026) ──
        {"date": "2026-02-28", "title": "Operation Epic Fury Begins: US & Israel Strike Iran; Khamenei Killed", "type": "military", "description": "Joint US-Israeli strikes at 2:30 AM EST. ~900 US strikes in 12 hours, 1,000+ targets in 24h. Supreme Leader Khamenei killed in Israeli strikes on Tehran compound. Iran retaliates with dozens of ballistic missiles and drones at Israel and US bases. US Embassy in Kuwait hit.", "severity": 4, "fatalities": 354, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-03-01", "title": "Maersk Suspends Strait of Hormuz Transit; 4 US Soldiers Killed", "type": "proxy", "description": "Maersk suspends all Strait of Hormuz crossings, reroutes around Cape of Good Hope. Tanker transits drop from 24/day to 4. Four US soldiers killed in Kuwait drone strike.", "severity": 3, "fatalities": 4, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-03-02", "title": "IRGC Closes Strait of Hormuz; Hezbollah Enters War", "type": "military", "description": "IRGC officially closes Strait of Hormuz, threatens any ship that passes. 150+ ships anchored outside. Hezbollah fires rockets at Israel; IDF invades southern Lebanon.", "severity": 4, "fatalities": 85, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-03-03", "title": "Global Shipping Suspended; Brent Surges 7.8%", "type": "proxy", "description": "CMA CGM, Hapag-Lloyd, MSC suspend strait transits. IRGC deploys anti-ship missile launchers to Qeshm Island at narrowest Hormuz point. Iranian drones hit Gulf infrastructure. Brent settles at $83.28 (+7.8%).", "severity": 3, "fatalities": 45, "lat": 26.8500, "lon": 55.9000, "location": "Strait of Hormuz"},
        {"date": "2026-03-04", "title": "IRGC Fast Boats Attack Greek Tanker in Hormuz", "type": "military", "description": "IRGC Navy fires RPGs at Greek-flagged VLCC 'Athena Glory' transiting Hormuz under US escort. USS Mason sinks two IRGC boats. First direct naval engagement since war began. Iranian civilian toll passes 1,100. Brent settles at $81.56 (-2.1%) as markets interpret US naval dominance as stabilizing.", "severity": 3, "fatalities": 128, "lat": 26.4500, "lon": 56.4000, "location": "Strait of Hormuz"},
        {"date": "2026-03-05", "title": "IRGC Mines Sink Neutral Vessel; Insurance Withdrawn for Hormuz", "type": "military", "description": "Indian-flagged MV Ganges Spirit strikes Iranian mine in Hormuz, first neutral vessel sunk. Lloyd's suspends all Hormuz hull coverage. Iran fires 500+ missiles, 2,000 drones total. NATO intercepts missile over Turkey. Brent at $88.59 (+8.6%, ~24% since war began).", "severity": 4, "fatalities": 154, "lat": 26.5200, "lon": 56.3500, "location": "Strait of Hormuz"},
        {"date": "2026-03-06", "title": "Iran Strikes Gulf States; Brent Surges to $95.74 (+8.1%)", "type": "military", "description": "Iran strikes Saudi Arabia, Kuwait, Qatar, Bahrain, UAE. Missile hits Jerusalem. US Navy SEALs board IRGC minelayer deploying mines in Hormuz shipping lanes; 23 mines neutralized. CENTCOM declares limited maritime corridor. Brent settles at $95.74 (+8.1%).", "severity": 4, "fatalities": 95, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-03-07", "title": "Iranian Submarine Fires Torpedo at US Destroyer", "type": "military", "description": "IRIN Kilo-class submarine fires torpedo at USS Halsey in Gulf of Oman — first submarine attack on US Navy since WWII. Torpedo malfunctions; P-8 Poseidon forces submarine to surface. Iran's missile capability reported down 90% by Pentagon.", "severity": 3, "fatalities": 180, "lat": 25.5000, "lon": 57.5000, "location": "Gulf of Oman"},
        {"date": "2026-03-08", "title": "Brent Breaks $100/bbl; Shahran Oil Depot and Bandar Abbas Hit", "type": "military", "description": "Israel hits Shahran oil depot near Tehran; toxic smoke over capital. US destroys Bandar Abbas naval base — 14 IRGC vessels sunk. CENTCOM: 'IRGC Navy capability eliminated.' Assembly of Experts names Mojtaba Khamenei new Supreme Leader. Brent breaks $100/bbl.", "severity": 3, "fatalities": 285, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-03-09", "title": "Brent Hits $119/bbl Intraday; IRGC Drones Strike Fujairah Tanker", "type": "military", "description": "IRGC Shahed-136 drones strike Japanese VLCC 'Nippon Maru' at Fujairah, world's 2nd-largest bunkering hub; force majeure declared. Saudi intercepts drone toward Shaybah oilfield. Cumulative toll: 2,400+ killed. Brent hits $119/bbl intraday but settles at $98.96 as Pentagon reports Iran's missile capability down 90% — markets pricing in war's end.", "severity": 3, "fatalities": 252, "lat": 25.1288, "lon": 56.3265, "location": "Fujairah, UAE"},
        {"date": "2026-03-10", "title": "Brent Drops 11.3% as Trump Signals War 'Very Complete'", "type": "diplomatic", "description": "Trump tells CBS war is 'very complete, pretty much.' Energy Secretary's false tanker escort claim triggers flash crash. US destroys 16 Iranian minelayers. Brent drops 11.3% to $87.80, largest single-day drop since March 2022.", "severity": 1, "fatalities": 200, "lat": 38.9072, "lon": -77.0369, "location": "Washington, DC"},
        {"date": "2026-03-11", "title": "Iranian Drones Strike Salalah Port, Oman; Shipping Route Threatened", "type": "military", "description": "Iranian drone strike sets fire to Salalah, Oman's largest commercial port — major escalation hitting neutral mediator state. Threatens critical non-Hormuz shipping route. Iran rejects ceasefire. Pentagon: 140 US troops wounded, 7 killed since Feb 28.", "severity": 3, "fatalities": 180, "lat": 17.0151, "lon": 54.0924, "location": "Salalah, Oman"},
        {"date": "2026-03-12", "title": "Brent Surges Past $105; Hormuz Transit Drops to 2 Ships/Day", "type": "proxy", "description": "Brent hits $105.40/bbl. Hormuz transit at 2 ships/day (was 24). IEA declares first 'severe supply disruption' since 2011. Houthis fire missiles at Saudi Ras Tanura oil terminal. IRGC anti-ship missiles hit tanker near Fujairah. 21% of global oil supply disrupted.", "severity": 4, "fatalities": 70, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-03-13", "title": "Mojtaba Khamenei Threatens Permanent Hormuz Closure", "type": "diplomatic", "description": "New Supreme Leader threatens to permanently seal the Strait of Hormuz. Analysts note IRGC Navy largely destroyed since March 8; uncleared sea mines remain primary residual threat.", "severity": 1, "fatalities": 0, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-03-14", "title": "Trump Orders Strikes on Kharg Island; 90% of Iran's Oil Exports", "type": "military", "description": "Trump orders strikes on Kharg Island, which handles 90% of Iran's crude oil exports. Major escalation targeting Iran's economic lifeline. Separate strikes hit Isfahan, damaging UNESCO-listed Chehel Sotoun Palace.", "severity": 4, "fatalities": 15, "lat": 29.2333, "lon": 50.3167, "location": "Kharg Island, Iran"},
        {"date": "2026-03-15", "title": "Iran FM Declares 'Ready for a Long War'; Rejects Ceasefire", "type": "diplomatic", "description": "Iranian FM Araghchi states Tehran 'never sought a ceasefire' and remains ready for a long war. Trump urges world to keep Hormuz open. Bahrain and Saudi Arabia cancel April F1 Grand Prix over safety.", "severity": 1, "fatalities": 0, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-03-16", "title": "Houthis Await Iran Approval to Resume Red Sea Attacks", "type": "proxy", "description": "Reports emerge that Houthis are awaiting Iranian approval to resume Red Sea shipping attacks if Iran's Hormuz control weakens. FM Araghchi rejects ceasefire. Lebanon death toll reaches 850 with 831,000 displaced.", "severity": 2, "fatalities": 50, "lat": 14.0, "lon": 44.0, "location": "Red Sea"},
        {"date": "2026-03-17", "title": "Israel Assassinates Larijani; Iran Retaliates Across Gulf States", "type": "military", "description": "Israel kills top security official Ali Larijani and Basij commander Gholamreza Soleimani. Iran retaliates with strikes at Saudi Arabia, Kuwait, UAE. Multiple missiles hit Tel Aviv, killing at least 2. Broadening regional escalation.", "severity": 3, "fatalities": 5, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-03-18", "title": "South Pars Gas Field Struck; Intelligence Minister Khatib Killed", "type": "military", "description": "Israel strikes South Pars gas field — world's largest natural gas reserve shared with Qatar. Iran takes several South Pars phases offline. Israel also confirms killing Intelligence Minister Esmail Khatib in Tehran, a day after assassinating Larijani and Basij chief Soleimani. Funeral processions for Larijani draw massive crowds in Tehran. Iran rejects ceasefire; Hormuz transit effectively halted.", "severity": 3, "fatalities": 0, "lat": 27.5000, "lon": 52.0000, "location": "South Pars, Persian Gulf"},

        # ── Day 20 (March 19, 2026) ──
        {"date": "2026-03-19", "title": "Brent Hits $115; Strikes on Yazd Airport; Ras Laffan LNG Hub Hit", "type": "military", "description": "Brent crude surges to $115/bbl after Israeli strikes spark Iranian retaliation on Qatar's Ras Laffan LNG hub. US-Israeli strikes hit Yazd Airport. Trump threatens to 'blow up' South Pars if Iran continues attacking Qatar. Saudi Arabia warns it will take military action against Iran. IDF has carried out 7,600+ strikes across Iran. Pentagon requests $200B+ war budget. FM Araghchi warns allies helping reopen Hormuz risk 'complicity in war crimes.'", "severity": 3, "fatalities": 0, "lat": 31.9049, "lon": 54.2825, "location": "Yazd, Iran"},

        # ── Days 21-30 (March 20-29, 2026) ──
        {"date": "2026-03-20", "title": "Iraq Declares Force Majeure on Basra Crude; Brent at $110.96", "type": "proxy", "description": "Iraq declares force majeure on Basra crude exports citing pipeline damage from Iranian retaliatory strikes near Fao Peninsula. Gulf tanker insurance premiums hit record highs. Brent settles at $110.96 as supply disruptions widen beyond Iran.", "severity": 2, "fatalities": 0, "lat": 30.5085, "lon": 47.7804, "location": "Basra, Iraq"},
        {"date": "2026-03-23", "title": "Brent Drops to $96; IRGC Hormuz 'Tollbooth' Rumors Emerge", "type": "diplomatic", "description": "Brent drops sharply to $96.07 as reports emerge that IRGC may allow select nations to transit Hormuz for fees — a de facto 'tollbooth' system. Markets interpret this as partial de-escalation. China and India reportedly negotiating safe passage for their flagged tankers.", "severity": 1, "fatalities": 0, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-03-25", "title": "Kuwait Airport Drone Attack; UNIFIL Peacekeeper Killed in Lebanon", "type": "military", "description": "Iranian-aligned militia drones strike Kuwait International Airport, damaging runway and terminal. One UNIFIL peacekeeper killed in southern Lebanon as Hezbollah-IDF fighting intensifies. Regional conflict spreading to previously neutral states.", "severity": 3, "fatalities": 3, "lat": 29.2266, "lon": 47.9689, "location": "Kuwait City, Kuwait"},
        {"date": "2026-03-26", "title": "Iran Rejects Direct US Talks; IRGC Opens Hormuz 'Tollbooth'", "type": "diplomatic", "description": "Iran formally rejects direct US negotiations, calling preconditions 'surrender terms.' IRGC confirms selective Hormuz transit for Chinese and Indian tankers paying transit fees — estimated $2-5M per passage. Western-flagged vessels still blocked. Brent rebounds to $108.01.", "severity": 2, "fatalities": 0, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-03-27", "title": "3,500 Marines Arrive on USS Tripoli; Humanitarian Ships Allowed", "type": "military", "description": "USS Tripoli arrives in Gulf of Oman with 3,500 Marines from 15th MEU. Pentagon announces humanitarian corridor through Hormuz for food and medical supplies. Iran allows passage of two humanitarian vessels as goodwill gesture while maintaining military blockade.", "severity": 2, "fatalities": 0, "lat": 25.0000, "lon": 57.5000, "location": "Gulf of Oman"},
        {"date": "2026-03-28", "title": "Houthis Fire Missiles at Israel; 82nd Airborne Deploys to Region", "type": "military", "description": "Houthis fire ballistic missiles at Israel for first time since war began — 11 injured from debris in southern Israel. Marks major Houthi escalation beyond Red Sea shipping attacks. 82nd Airborne Division deploys to region. Brent surges to $112.57. Renewed fears of Red Sea shipping strikes as Houthis re-enter direct combat.", "severity": 3, "fatalities": 0, "lat": 31.0461, "lon": 34.8516, "location": "Southern Israel"},
        {"date": "2026-03-29", "title": "Pentagon Prepares Ground Operations in Iran; 13 US KIA Total", "type": "military", "description": "Pentagon announces preparation for 'weeks of ground operations' targeting Qeshm Island and Kharg Island to permanently secure Hormuz and eliminate Iran's oil export capability. 82nd Airborne and Marine forces staging. Iran's IRGC threatens to 'rain fire' on any ground troops. US casualties now 13 KIA, 300+ wounded since Feb 28. Oil markets brace for further escalation.", "severity": 3, "fatalities": 0, "lat": 26.8500, "lon": 55.9000, "location": "Qeshm Island, Iran"},

        # ── Days 32-40 (March 31 – April 8, 2026) ──
        {"date": "2026-03-31", "title": "Continued Airstrikes Across Iran; Parliament Rejects Negotiations", "type": "military", "description": "US and Israeli air strikes continue with explosions reported in Tehran and Isfahan. Secretary Rubio says war objectives will be achieved in 'weeks, not months.' At least 4,700 Iranian security forces killed. Iranian Parliament Speaker Ghalibaf rejects negotiations, saying Iran cannot be forced into submission.", "severity": 3, "fatalities": 0, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-04-01", "title": "UK Announces 35-Nation Hormuz Meeting; Argentina Designates IRGC Terrorist Org", "type": "diplomatic", "description": "Britain announces meeting of ~35 countries to discuss reopening the Strait of Hormuz. China and Pakistan propose five-point ceasefire plan. Argentina designates IRGC as terrorist organization. Ships continue paying Iran tolls in yuan and cryptocurrency for Hormuz passage.", "severity": 1, "fatalities": 0, "lat": 51.5074, "lon": -0.1278, "location": "London, UK"},
        {"date": "2026-04-02", "title": "UK-Led 35-Nation Coalition Meets on Hormuz; Trump Says Objectives Nearly Met", "type": "diplomatic", "description": "UK Foreign Secretary Cooper chairs virtual meeting of ~35 countries to restore Hormuz freedom of navigation. US does not attend. PM Starmer calls for 'united front of military strength and diplomatic activity.' Trump states Washington close to achieving objectives. Hormuz evolves into dual-corridor system.", "severity": 1, "fatalities": 0, "lat": 51.5074, "lon": -0.1278, "location": "London, UK"},
        {"date": "2026-04-03", "title": "US F-15E Shot Down Over Iran; Search and Rescue Launched", "type": "military", "description": "American F-15E Strike Eagle shot down by Iranian forces. One crew member rescued alive, search continues for weapons system officer. An A-10 supporting the rescue is also struck; pilot ejects over Kuwait. Two UH-60 Black Hawks hit with minor injuries. Separately, US-Israel strikes a medical research center and bridge near Tehran.", "severity": 3, "fatalities": 1, "lat": 33.0000, "lon": 52.0000, "location": "Central Iran"},
        {"date": "2026-04-04", "title": "Projectile Strikes Near Bushehr Nuclear Plant; IAEA Issues Warning", "type": "nuclear", "description": "IAEA confirms projectile struck close to Bushehr nuclear power plant — fourth such incident. One protection staff member killed by fragment. No radiation increase detected. IAEA Director General Grossi: nuclear sites 'must never be attacked.' Russia evacuates 198 workers from facility.", "severity": 3, "fatalities": 1, "lat": 28.8333, "lon": 50.8833, "location": "Bushehr, Iran"},
        {"date": "2026-04-05", "title": "Trump Threatens Iran's Power Grid; Downed Pilot Rescued; Iraqi Militia Attacks US Embassy", "type": "military", "description": "Trump threatens to attack Iran's power plants and bridges if Hormuz not reopened within two days. Iranian-backed Iraqi militia Saraya Awliya al-Dam attacks US diplomatic facilities in Baghdad. Second F-15E crew member rescued alive. Iranian UAVs and cruise missiles intercepted in Qatari airspace.", "severity": 3, "fatalities": 0, "lat": 33.3152, "lon": 44.3661, "location": "Baghdad, Iraq"},
        {"date": "2026-04-06", "title": "Pakistan Proposes 'Islamabad Accord' — 45-Day Ceasefire Plan", "type": "diplomatic", "description": "Pakistan offers two-phased 45-day truce ('Islamabad Accord'). Pakistan army chief in contact with VP Vance, envoy Witkoff, and Iranian FM Araghchi. Iran 'positively reviewing' proposal but refuses to reopen Hormuz under temporary ceasefire. Houthis threaten to close Bab al-Mandeb strait — fears of second chokepoint crisis.", "severity": 2, "fatalities": 0, "lat": 33.6844, "lon": 73.0479, "location": "Islamabad, Pakistan"},
        {"date": "2026-04-07", "title": "Trump: 'A Whole Civilization Will Die Tonight'; Ceasefire Announced Before Deadline", "type": "diplomatic", "description": "Trump sets 8 PM ET deadline, posts 'a whole civilization will die tonight.' White House denies nuclear weapon plans. Pakistan PM makes last-ditch appeal. ~90 minutes before deadline, Trump announces two-week ceasefire based on Pakistan-mediated 10-point proposal. Iran to reopen Strait of Hormuz.", "severity": 2, "fatalities": 0, "lat": 38.9072, "lon": -77.0369, "location": "Washington, DC"},
        {"date": "2026-04-08", "title": "Islamabad Accords Ceasefire Takes Effect; Israel Launches 'Eternal Darkness' on Lebanon", "type": "diplomatic", "description": "Two-week ceasefire takes effect. Iran's Supreme National Security Council confirms acceptance. Hezbollah announces pause. But Israel launches 'Operation Eternal Darkness' — 100 airstrikes on Lebanon in 10 minutes targeting Hezbollah HQ and missile infrastructure. Gulf states intercept missiles; fires at Abu Dhabi's Habshan gas complex and Saudi pipeline. Hormuz remains effectively closed — 800+ freighters stuck. Brent crashes ~13% from pre-ceasefire highs.", "severity": 2, "fatalities": 0, "lat": 33.6844, "lon": 73.0479, "location": "Islamabad, Pakistan"},

        # ── Ceasefire Period (April 9-13, 2026) ──
        {"date": "2026-04-09", "title": "Ceasefire Violations Begin; Hormuz Still Closed; Only 4 Ships Transit", "type": "military", "description": "No sign of Hormuz blockade lifting — Iran limits crossings, charges tolls over $1M per ship. Only 4 dry cargo ships pass through. Iran accuses US/Israel of ceasefire violations over Lebanon strikes. IDF's 98th Division takes Bint Jbeil in southern Lebanon. Hezbollah fires rockets at Kiryat Shmona. 230 loaded oil tankers stuck inside the Gulf. Brent at $100.99.", "severity": 2, "fatalities": 0, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-04-10", "title": "Trump Accuses Iran of 'Very Poor Job' on Hormuz; Only 15 Ships Through", "type": "diplomatic", "description": "Trump accuses Iran of doing 'very poor job' managing Hormuz oil flow, says situation is 'not the agreement we have.' Only 15 ships total have made it through strait since ceasefire. Israel strikes Lebanon in early morning; Hezbollah launches rockets at Metula. Sirens in Tel Aviv, Haifa, and Ashdod. Ceasefire effectively collapsing. Brent at $97.78.", "severity": 2, "fatalities": 0, "lat": 38.9072, "lon": -77.0369, "location": "Washington, DC"},
        {"date": "2026-04-11", "title": "Vance Arrives in Islamabad for Peace Talks; US Navy Enters Hormuz", "type": "diplomatic", "description": "VP Vance, envoy Witkoff, and Jared Kushner arrive in Islamabad for talks with Iranian FM Araghchi and Parliament Speaker Ghalibaf. Several US Navy destroyers enter Strait of Hormuz for first time since war began — CENTCOM says mine clearance operations. Iran threatens to attack ships, accuses US of ceasefire violation. Trump announces US forces 'clearing' the strait.", "severity": 2, "fatalities": 0, "lat": 33.6844, "lon": 73.0479, "location": "Islamabad, Pakistan"},
        {"date": "2026-04-12", "title": "Islamabad Talks Collapse After 21 Hours; Trump Declares Naval Blockade", "type": "diplomatic", "description": "Vance leaves Pakistan after 21-hour marathon talks without agreement. Iranian FM spokesman says 'gaps on several major issues.' Ghalibaf says US 'did not succeed in gaining trust.' Trump threatens 'full naval blockade' — CENTCOM announces blockade starting Monday 10 AM EDT targeting vessels from/to Iranian ports. Non-Iran traffic free to transit. Brent at $96.", "severity": 2, "fatalities": 0, "lat": 33.6844, "lon": 73.0479, "location": "Islamabad, Pakistan"},
        {"date": "2026-04-13", "title": "US Naval Blockade Imminent; Oil Surges on Escalation Fears", "type": "military", "description": "Markets react to impending US naval blockade of Strait of Hormuz. Brent surges 6.95% to $103.72 as traders price in blockade risk. 500-700 vessels over 10,000 DWT stuck in Persian Gulf. Poll shows 63% of Israelis believe ceasefire does not extend to Lebanon. Ceasefire increasingly fragile ahead of Monday blockade deadline.", "severity": 2, "fatalities": 0, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},

        # ── Naval Blockade & Hormuz Reopening (April 14-17, 2026) ──
        {"date": "2026-04-14", "title": "US Naval Blockade Takes Effect; Trump Hints Talks Could Resume", "type": "military", "description": "US naval blockade of Iranian ports goes into force at 10 AM ET. CENTCOM says blockade applies to vessels entering/departing Iranian ports but 'will not impede freedom of navigation for vessels transiting the Strait of Hormuz to and from non-Iranian ports.' Trump hints US-Iran talks could resume within two days. White House confirms more peace deal talks in discussion. Brent at $100.19.", "severity": 2, "fatalities": 0, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-04-15", "title": "Trump: Iran War 'Very Close to Over'; Israel-Lebanon Negotiations Advance", "type": "diplomatic", "description": "Day 47 of conflict. Trump says Iran war 'very close to over,' emphasizes Iran eager to make a deal and has 'agreed to things it hadn't two months ago.' Active Israel-Lebanon ceasefire negotiations underway. Brent drops to $96.83 on diplomatic optimism.", "severity": 1, "fatalities": 0, "lat": 38.9072, "lon": -77.0369, "location": "Washington, DC"},
        {"date": "2026-04-16", "title": "13 Iranian Oil Tankers Intercepted; Israel-Lebanon 10-Day Truce Announced", "type": "diplomatic", "description": "Gen. Dan Caine (JCS Chairman) announces 13 oil tankers intercepted enforcing Iran blockade. Trump announces Israel-Lebanon 10-day truce — separate from Iran negotiations. Trump: Israel 'prohibited' from bombing Lebanon during truce. Brent ends at $94.89 as Lebanon ceasefire eases regional tensions.", "severity": 1, "fatalities": 0, "lat": 33.8938, "lon": 35.5018, "location": "Beirut, Lebanon"},
        {"date": "2026-04-17", "title": "STRAIT OF HORMUZ REOPENED FOR COMMERCIAL SHIPPING", "type": "diplomatic", "description": "Iran FM Araghchi declares Strait of Hormuz 'completely open for commercial ships for remainder of ceasefire' — explicitly tied to Lebanon 10-day truce duration. US blockade of Iranian ports remains in full force pending peace deal. Starmer-Trump call on assembling diplomatic coalition for long-term Hormuz reopening. France/UK rally 40+ nations on defensive plan. Ceasefire continues. Partial breakthrough but full normalization contingent on broader peace deal.", "severity": 1, "fatalities": 0, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
    ]


def fetch_iran_news() -> List[dict]:
    """Fetch live Iran/oil war news headlines from Google News RSS. No API key needed."""
    cached = _read_cache("iran_news", 7200)  # 30-minute cache
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
            cached_news = _read_cache("iran_news", 7200)
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


# ─── Iran Conflict Intensity Index ─────────────────────────────────────────────

def compute_iran_intensity() -> List[dict]:
    """Build a daily/weekly Iran conflict intensity index from ACLED + curated events.
    Analogous to WeeklyAttackFreq from the Houthi thesis analysis.

    Scoring:
      - ACLED events: Battles/Explosions=3, Violence against civilians=2, other=1
      - Curated events: weighted by severity (1-5)
      - Fatalities: log-scaled bonus
      - Hormuz proximity (<200km): 2x multiplier
    """
    # Get data sources
    iran_acled = _read_cache("iran_events", 7200) or _load_iran_json_fallback() or []
    curated = get_curated_iran_events()

    # ACLED event type weights
    acled_weights = {
        "Battles": 3, "Explosions/Remote violence": 3,
        "Violence against civilians": 2, "Strategic developments": 1,
        "Protests": 1, "Riots": 1,
    }

    # Hormuz coordinates for proximity check
    HORMUZ_LAT, HORMUZ_LON = 26.5667, 56.2500

    def _haversine_km(lat1, lon1, lat2, lon2):
        R = 6371
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
        return R * 2 * math.asin(math.sqrt(a))

    # Build daily scores
    daily_scores = {}

    # Score ACLED events
    for e in iran_acled:
        date = (e.get("event_date") or "")[:10]
        if not date or date < "2025-01-01":
            continue
        etype = e.get("event_type", "")
        weight = acled_weights.get(etype, 1)
        fatalities = int(e.get("fatalities", 0) or 0)
        fat_bonus = math.log1p(fatalities) if fatalities > 0 else 0

        # Proximity multiplier
        try:
            lat = float(e.get("latitude", 0))
            lon = float(e.get("longitude", 0))
            dist = _haversine_km(lat, lon, HORMUZ_LAT, HORMUZ_LON)
            proximity = 2.0 if dist < 200 else 1.0
        except (ValueError, TypeError):
            proximity = 1.0

        score = (weight + fat_bonus) * proximity
        daily_scores[date] = daily_scores.get(date, 0) + score

    # Score curated events (higher weight — these are confirmed major events)
    for e in curated:
        date = e["date"]
        severity = e.get("severity", 1)
        fatalities = e.get("fatalities", 0) or 0
        fat_bonus = math.log1p(fatalities) if fatalities > 0 else 0

        try:
            lat = float(e.get("lat", 0))
            lon = float(e.get("lon", 0))
            dist = _haversine_km(lat, lon, HORMUZ_LAT, HORMUZ_LON)
            proximity = 2.0 if dist < 200 else 1.0
        except (ValueError, TypeError):
            proximity = 1.0

        score = (severity * 2 + fat_bonus) * proximity
        daily_scores[date] = daily_scores.get(date, 0) + score

    # Build time series with 7-day rolling sum
    if not daily_scores:
        return []

    sorted_dates = sorted(daily_scores.keys())
    start = datetime.strptime(sorted_dates[0], "%Y-%m-%d")
    end = datetime.strptime(sorted_dates[-1], "%Y-%m-%d")

    result = []
    current = start
    window = []
    while current <= end:
        ds = current.strftime("%Y-%m-%d")
        daily = round(daily_scores.get(ds, 0), 2)
        window.append(daily)
        if len(window) > 7:
            window.pop(0)
        weekly = round(sum(window), 2)
        result.append({"date": ds, "daily_intensity": daily, "weekly_intensity": weekly})
        current += timedelta(days=1)

    return result


def compute_hormuz_disruption() -> dict:
    """Compare current Hormuz transit rates to pre-war baseline."""
    hormuz = _read_cache("hormuz_transits", 86400)
    if not hormuz:
        try:
            hormuz = _fetch_chokepoint_transits("hormuz")
        except Exception:
            hormuz = []

    if not hormuz or len(hormuz) < 3:
        return {"status": "UNKNOWN", "baseline_transits": None, "current_transits": None, "pct_decline": None}

    # Pre-war baseline: average of months before Feb 2026
    pre_war = [m for m in hormuz if m["month"] < "2026-02"]
    current = [m for m in hormuz if m["month"] >= "2026-02"]

    if not pre_war or not current:
        return {"status": "UNKNOWN", "baseline_transits": None, "current_transits": None, "pct_decline": None}

    baseline_avg = sum(m["transits"] for m in pre_war) / len(pre_war)
    current_avg = sum(m["transits"] for m in current) / len(current)
    pct_decline = round((1 - current_avg / baseline_avg) * 100, 1) if baseline_avg > 0 else 0

    # Tanker-specific if available
    tanker_baseline = None
    tanker_current = None
    tanker_decline = None
    if pre_war[0].get("tanker_transits") is not None:
        tanker_baseline = sum(m.get("tanker_transits", 0) for m in pre_war) / len(pre_war)
        tanker_current = sum(m.get("tanker_transits", 0) for m in current) / len(current)
        tanker_decline = round((1 - tanker_current / tanker_baseline) * 100, 1) if tanker_baseline > 0 else 0

    # Status classification — PortWatch data lags. Override with current events.
    today = datetime.now().strftime("%Y-%m-%d")

    # As of Apr 17, 2026: Iran declared Hormuz open for remainder of Lebanon ceasefire
    status_note = None
    if today >= "2026-04-17":
        status = "CONDITIONAL"
        status_note = "Iran declared Hormuz 'completely open for commercial ships' on Apr 17 (tied to 10-day Lebanon ceasefire). US blockade of Iranian ports remains in force pending peace deal."
    elif today >= "2026-04-13":
        status = "BLOCKADE"
        status_note = "US naval blockade enforcing against Iranian ports (non-Iran traffic permitted in strait)."
    elif pct_decline >= 80:
        status = "BLOCKADE"
    elif pct_decline >= 40:
        status = "RESTRICTED"
    elif pct_decline >= 10:
        status = "DISRUPTED"
    else:
        status = "OPEN"

    return {
        "status": status,
        "status_note": status_note,
        "baseline_transits": round(baseline_avg),
        "current_transits": round(current_avg),
        "pct_decline": pct_decline,
        "tanker_baseline": round(tanker_baseline) if tanker_baseline else None,
        "tanker_current": round(tanker_current) if tanker_current else None,
        "tanker_pct_decline": tanker_decline,
        "months_data": hormuz,
    }


def get_war_phases() -> List[dict]:
    """Return war phase definitions for timeline annotations."""
    return [
        {"phase": 1, "name": "Maximum Pressure Restored", "start": "2025-01-20", "end": "2026-02-27", "color": "#F09060"},
        {"phase": 2, "name": "Twelve-Day War", "start": "2026-02-28", "end": "2026-03-11", "color": "#E05555"},
        {"phase": 3, "name": "Hormuz Blockade & Escalation", "start": "2026-03-12", "end": "2026-03-19", "color": "#9B8EC4"},
        {"phase": 4, "name": "Regional Spread", "start": "2026-03-20", "end": "2026-03-29", "color": "#D4B870"},
        {"phase": 5, "name": "Ground Operations Prep", "start": "2026-03-30", "end": "2026-04-07", "color": "#E05555"},
        {"phase": 6, "name": "Ceasefire & Islamabad Talks", "start": "2026-04-08", "end": "2026-04-12", "color": "#4A90D9"},
        {"phase": 7, "name": "Naval Blockade", "start": "2026-04-13", "end": "2026-04-16", "color": "#E05555"},
        {"phase": 8, "name": "Hormuz Reopened (Conditional)", "start": "2026-04-17", "end": "2026-12-31", "color": "#00e676"},
    ]


def get_comparative_data() -> dict:
    """Build Houthi vs Iran comparative analysis dataset."""
    # Houthi data from master dataset CSV
    thesis_df = None
    try:
        thesis_df = pd.read_csv(config.MYLES_DATASET_PATH)
    except Exception as e:
        logger.warning(f"Failed to load master dataset for comparative: {e}")
    houthi_ts = []
    if thesis_df is not None and not thesis_df.empty:
        for _, row in thesis_df.iterrows():
            houthi_ts.append({
                "date": str(row.get("Date", ""))[:10],
                "brent_price": float(row["Brent_Price"]) if pd.notna(row.get("Brent_Price")) else None,
                "weekly_attacks": float(row["WeeklyAttackFreq"]) if pd.notna(row.get("WeeklyAttackFreq")) else None,
                "daily_volatility": float(row["Daily_Volatility"]) if pd.notna(row.get("Daily_Volatility")) else None,
            })

    # Iran intensity data
    iran_intensity = compute_iran_intensity()

    # Brent prices (includes war period)
    brent = _read_cache("brent_prices", 7200) or []

    # Hormuz disruption
    hormuz = compute_hormuz_disruption()

    # Chokepoint transit comparison
    bab_transits = _read_cache("bab_el_mandeb_transits", 86400) or []
    hormuz_transits = hormuz.get("months_data", [])

    # War phases
    phases = get_war_phases()

    # Houthi period stats
    houthi_prices = [h["brent_price"] for h in houthi_ts if h["brent_price"]]
    houthi_vol = [h["daily_volatility"] for h in houthi_ts if h["daily_volatility"]]

    # Iran period stats (from curated events + Brent)
    iran_brent = [b for b in brent if b["date"] >= "2026-02-28"]
    iran_prices = [b["price"] for b in iran_brent]

    return {
        "houthi_timeseries": houthi_ts,
        "iran_intensity": iran_intensity,
        "iran_brent": iran_brent,
        "bab_transits": bab_transits,
        "hormuz_transits": hormuz_transits,
        "hormuz_disruption": hormuz,
        "war_phases": phases,
        "summary": {
            "houthi_period": "Oct 2023 – Oct 2025",
            "houthi_price_range": [round(min(houthi_prices), 2), round(max(houthi_prices), 2)] if houthi_prices else None,
            "houthi_avg_volatility": round(sum(houthi_vol) / len(houthi_vol), 4) if houthi_vol else None,
            "iran_period": "Feb 28, 2026 – Present",
            "iran_price_range": [round(min(iran_prices), 2), round(max(iran_prices), 2)] if iran_prices else None,
            "iran_price_change_pct": round((iran_prices[-1] - iran_prices[0]) / iran_prices[0] * 100, 1) if len(iran_prices) >= 2 else None,
            "thesis_finding": "Houthi attacks had minimal impact on oil volatility (market adaptation). Iran war caused massive price disruption — state-actor chokepoint closure breaks the adaptation model.",
        },
    }


def _t_cdf_approx(t: float, df: int) -> float:
    """Approximate Student's t-distribution CDF using a series expansion.
    Good enough for p-value calculation without requiring scipy.

    For df > 30, normal approximation is very accurate.
    For smaller df, uses the relation to the incomplete beta function
    via a continued fraction approximation.
    """
    if df <= 0:
        return 0.5

    # For large df, use normal approximation via erf
    if df > 30:
        return 0.5 * (1 + math.erf(t / math.sqrt(2)))

    # For smaller df, use the relation: P(T <= t) = 1 - 0.5 * I(df/(df+t^2); df/2, 0.5)
    # where I is the regularized incomplete beta function.
    # We use an approximation from Abramowitz & Stegun.
    x = df / (df + t * t)

    # Regularized incomplete beta via continued fraction (Lentz's method)
    def _incbeta(a, b, x):
        if x <= 0:
            return 0.0
        if x >= 1:
            return 1.0
        # Use continued fraction
        bt = math.exp(math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) +
                      a * math.log(x) + b * math.log(1 - x))
        if x < (a + 1) / (a + b + 2):
            return bt * _betacf(a, b, x) / a
        else:
            return 1 - bt * _betacf(b, a, 1 - x) / b

    def _betacf(a, b, x):
        MAXIT = 200
        EPS = 3e-7
        qab = a + b
        qap = a + 1
        qam = a - 1
        c = 1.0
        d = 1.0 - qab * x / qap
        if abs(d) < 1e-30:
            d = 1e-30
        d = 1.0 / d
        h = d
        for m in range(1, MAXIT + 1):
            m2 = 2 * m
            aa = m * (b - m) * x / ((qam + m2) * (a + m2))
            d = 1.0 + aa * d
            if abs(d) < 1e-30:
                d = 1e-30
            c = 1.0 + aa / c
            if abs(c) < 1e-30:
                c = 1e-30
            d = 1.0 / d
            h *= d * c
            aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
            d = 1.0 + aa * d
            if abs(d) < 1e-30:
                d = 1e-30
            c = 1.0 + aa / c
            if abs(c) < 1e-30:
                c = 1e-30
            d = 1.0 / d
            dl = d * c
            h *= dl
            if abs(dl - 1.0) < EPS:
                break
        return h

    try:
        ib = _incbeta(df / 2, 0.5, x)
        if t >= 0:
            return 1 - 0.5 * ib
        else:
            return 0.5 * ib
    except Exception:
        # Fallback to normal approximation
        return 0.5 * (1 + math.erf(t / math.sqrt(2)))


def _two_sided_pvalue(t: float, df: int) -> float:
    """Two-sided p-value for a t-statistic."""
    cdf = _t_cdf_approx(abs(t), df)
    return max(0.0, min(1.0, 2 * (1 - cdf)))


def _ols_simple(x: np.ndarray, y: np.ndarray) -> dict:
    """Pure-numpy simple linear regression. Returns slope, intercept, SE, p, R²."""
    n = len(x)
    if n < 3:
        return None
    x_mean = x.mean()
    y_mean = y.mean()
    ss_xx = ((x - x_mean) ** 2).sum()
    ss_xy = ((x - x_mean) * (y - y_mean)).sum()
    ss_yy = ((y - y_mean) ** 2).sum()

    if ss_xx == 0 or ss_yy == 0:
        return None

    slope = ss_xy / ss_xx
    intercept = y_mean - slope * x_mean
    r_sq = (ss_xy ** 2) / (ss_xx * ss_yy)

    # Residuals and standard error
    y_hat = intercept + slope * x
    residuals = y - y_hat
    sse = (residuals ** 2).sum()
    mse = sse / (n - 2) if n > 2 else 0
    se_slope = math.sqrt(mse / ss_xx) if ss_xx > 0 else 0

    # t-stat and p-value (two-sided)
    t_stat = slope / se_slope if se_slope > 0 else 0
    p_val = _two_sided_pvalue(t_stat, df=n - 2)

    return {
        "coefficient": float(slope),
        "intercept": float(intercept),
        "std_error": float(se_slope),
        "t_stat": float(t_stat),
        "p_value": float(p_val),
        "r_squared": float(r_sq),
        "n_obs": int(n),
    }


def run_iran_regression() -> dict:
    """Run OLS regression on Iran war data: daily volatility ~ conflict intensity.
    Pure-numpy implementation — no scipy required.
    """
    # Get data
    intensity = compute_iran_intensity()
    brent = _read_cache("brent_prices", 7200) or []

    if not intensity or len(brent) < 5:
        return {"error": "Insufficient data", "n_obs": 0}

    # Build Brent price map and compute daily volatility
    price_map = {b["date"]: b["price"] for b in brent}
    sorted_dates = sorted(price_map.keys())

    vol_map = {}
    for i in range(1, len(sorted_dates)):
        d = sorted_dates[i]
        prev = sorted_dates[i - 1]
        change = abs((price_map[d] - price_map[prev]) / price_map[prev]) * 100
        vol_map[d] = change

    intensity_map = {i["date"]: i["weekly_intensity"] for i in intensity}
    daily_intensity_map = {i["date"]: i["daily_intensity"] for i in intensity}

    war_dates = [d for d in sorted_dates if d >= "2026-02-28" and d in vol_map and d in intensity_map]

    if len(war_dates) < 5:
        return {"error": "Insufficient war-period observations", "n_obs": len(war_dates)}

    y = np.array([vol_map[d] for d in war_dates])
    x_weekly = np.array([intensity_map[d] for d in war_dates])
    x_daily = np.array([daily_intensity_map.get(d, 0) for d in war_dates])

    # Simple OLS models
    simple_weekly = _ols_simple(x_weekly, y)
    simple_daily = _ols_simple(x_daily, y)

    # Round for display
    def _round_result(r):
        if not r:
            return None
        return {k: (round(v, 6) if isinstance(v, float) else v) for k, v in r.items()}

    simple_result = _round_result(simple_weekly)
    daily_result = _round_result(simple_daily)

    # ── Multivariate OLS with DXY and OVX controls ──
    dxy_data = _read_cache("dxy", 7200) or []
    ovx_data = _read_cache("ovx", 7200) or []
    dxy_map = {d["date"]: d["value"] for d in dxy_data} if dxy_data else {}
    ovx_map = {d["date"]: d["value"] for d in ovx_data} if ovx_data else {}

    multi_dates = [d for d in war_dates if d in dxy_map and d in ovx_map]
    multi_result = None

    if len(multi_dates) >= 8:
        y_m = np.array([vol_map[d] for d in multi_dates])
        X = np.column_stack([
            np.ones(len(multi_dates)),
            [intensity_map[d] for d in multi_dates],
            [dxy_map[d] for d in multi_dates],
            [ovx_map[d] for d in multi_dates],
        ])

        try:
            betas, _, _, _ = np.linalg.lstsq(X, y_m, rcond=None)
            y_hat = X @ betas
            ss_res = np.sum((y_m - y_hat) ** 2)
            ss_tot = np.sum((y_m - np.mean(y_m)) ** 2)
            r_sq = 1 - ss_res / ss_tot if ss_tot > 0 else 0
            n, k = X.shape
            mse = ss_res / (n - k) if n > k else 0

            try:
                cov = mse * np.linalg.inv(X.T @ X)
                se = np.sqrt(np.diag(cov))
                t_stats = betas / se
                p_vals = [_two_sided_pvalue(float(t), df=n - k) for t in t_stats]
            except np.linalg.LinAlgError:
                se = [None] * k
                p_vals = [None] * k

            multi_result = {
                "labels": ["Intercept", "Iran Intensity", "DXY", "OVX"],
                "coefficients": [round(float(b), 6) for b in betas],
                "std_errors": [round(float(s), 6) if s is not None else None for s in se],
                "p_values": [round(float(p), 4) if p is not None else None for p in p_vals],
                "r_squared": round(float(r_sq), 4),
                "n_obs": int(n),
            }
        except Exception as e:
            logger.warning(f"Multivariate regression failed: {e}")

    return {
        "simple": simple_result,
        "daily": daily_result,
        "multivariate": multi_result,
        "war_dates": war_dates[:3] + ["..."] + war_dates[-3:] if len(war_dates) > 6 else war_dates,
        "date_range": f"{war_dates[0]} to {war_dates[-1]}",
        "caveat": f"Based on {len(war_dates)} trading days. Small sample — interpret with caution. Thesis used 505 observations over 24 months.",
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
        },
        "control_coefficients": {
            "h1": {
                "labels": ["Attack Freq (H1)", "DXY", "OVX", "SPR", "OPEC", "Russia-Ukraine", "China PMI", "Baker Hughes Rigs"],
                "coefficients": [-0.033, -0.134, 0.105, 0.000001, -0.318, -0.432, 0.173, 0.025],
                "std_errors": [0.015, 0.039, 0.013, 0.00000268, 0.256, 0.208, 0.162, 0.007],
                "p_values": [0.026, 0.001, 0.0001, 0.70, 0.21, 0.038, 0.29, 0.0004],
                "significant": [True, True, True, False, False, True, False, True]
            }
        }
    }
