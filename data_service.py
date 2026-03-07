"""
Data Service Layer - API integration with caching and CSV fallback.
Handles: ACLED, EIA (Brent + SPR), yfinance (DXY, OVX), FRED (China PMI)
"""
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict

import pandas as pd
import numpy as np
import requests

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
    path.write_text(json.dumps({"_ts": time.time(), "payload": payload}, default=str))


# ─── ACLED API ───────────────────────────────────────────────────────────────

_acled_token = None
_acled_token_expires = 0
_iran_fetch_error = None  # Store last error for debugging


def _get_acled_token() -> str:
    global _acled_token, _acled_token_expires
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
        timeout=30,
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


_ACLED_FIELDS = "event_id_cnty|event_date|event_type|sub_event_type|actor1|actor2|country|location|latitude|longitude|notes|fatalities|tags|source|source_scale"
_ACLED_DATE_RANGE = "2023-10-01|2026-12-31"

# Red Sea maritime keywords for filtering regional events.
# Only very specific terms — avoids false matches from generic conflict words.
_MARITIME_KEYWORDS = [
    "houthi", "ansar allah", "red sea", "bab el-mandeb", "bab al-mandab",
    "gulf of aden", "maritime", "shipping", "vessel", "tanker", "cargo ship",
    "oil tanker", "commercial ship", "merchant vessel", "container ship",
    "strait of hormuz", "suez canal", "usns", "uss ",
    "piracy", "hijack", "sea route", "waterway", "blockade",
    "coast guard", "naval blockade", "naval operation",
]


def _paginated_acled_fetch(token: str, params: dict, label: str) -> List[dict]:
    """Fetch paginated ACLED results."""
    results = []
    for page in range(1, 30):
        p = {**params, "page": page}
        resp = requests.get(
            config.ACLED_DATA_URL,
            headers={**_BROWSER_HEADERS, "Authorization": f"Bearer {token}"},
            params=p,
            timeout=60,
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

        # Query 1: All Yemen events
        yemen = _paginated_acled_fetch(token, {**base_params, "country": "Yemen"}, "Yemen")
        n = _add_unique(yemen)
        logger.info(f"ACLED Q1 Yemen: {n} unique events")

        # Query 2: Houthi actor events globally (captures Red Sea / Indian Ocean attacks)
        houthi = _paginated_acled_fetch(token, {
            **base_params, "actor1": "Houthi", "actor1_where": "LIKE",
        }, "Houthi-actor")
        n = _add_unique(houthi)
        logger.info(f"ACLED Q2 Houthi actor: {n} new unique events")

        # Query 3: Ansar Allah actor events (alternate name)
        ansar = _paginated_acled_fetch(token, {
            **base_params, "actor1": "Ansar Allah", "actor1_where": "LIKE",
        }, "AnsarAllah-actor")
        n = _add_unique(ansar)
        logger.info(f"ACLED Q3 Ansar Allah actor: {n} new unique events")

        # Query 4: Red Sea regional countries — filtered for maritime relevance
        for country in ["Saudi Arabia", "Djibouti", "Eritrea", "Oman", "Somalia", "Egypt", "Sudan", "Jordan", "Israel"]:
            regional = _paginated_acled_fetch(token, {
                **base_params, "country": country,
            }, country)
            maritime = [e for e in regional if _is_maritime_relevant(e)]
            n = _add_unique(maritime)
            logger.info(f"ACLED Q4 {country}: {len(regional)} total, {len(maritime)} maritime, {n} new")

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

    # Fall back to CSV files
    logger.info("ACLED: falling back to CSV")
    for path in [config.HOUTHI_CSV_PATH, config.HOUTHI_EXCEL_PATH]:
        if path.exists():
            try:
                if str(path).endswith(".xlsx"):
                    df = pd.read_excel(path)
                else:
                    df = pd.read_csv(path)

                col_map = {c: c.lower() for c in df.columns}
                df.rename(columns=col_map, inplace=True)

                records = []
                for _, row in df.iterrows():
                    records.append({
                        "event_id_cnty": str(row.get("event_id_cnty", "")),
                        "event_date": str(row.get("event_date", "")),
                        "event_type": str(row.get("event_type", "")),
                        "sub_event_type": str(row.get("sub_event_type", "")),
                        "actor1": str(row.get("actor1", "")),
                        "actor2": str(row.get("actor2", "")),
                        "country": str(row.get("country", "")),
                        "location": str(row.get("location", "")),
                        "latitude": float(row.get("latitude", 0)) if pd.notna(row.get("latitude")) else None,
                        "longitude": float(row.get("longitude", 0)) if pd.notna(row.get("longitude")) else None,
                        "notes": str(row.get("notes", "")),
                        "fatalities": int(row.get("fatalities", 0)) if pd.notna(row.get("fatalities")) else 0,
                        "tags": str(row.get("tags", "")),
                        "source": str(row.get("source", "")),
                        "source_scale": str(row.get("source_scale", "")),
                    })
                logger.info(f"ACLED CSV fallback: loaded {len(records)} events from {path.name}")
                return records
            except Exception as e:
                logger.warning(f"Failed to load {path}: {e}")
    return []


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
            timeout=30,
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
    """Extend EIA/FRED Brent data with recent prices beyond API reporting lag."""
    last_date = max(r["date"] for r in eia_records) if eia_records else "2023-10-01"

    # Try yfinance first
    try:
        import yfinance as yf
        df = yf.download("BZ=F", start=last_date, progress=False)
        if not df.empty:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            yf_records = [
                {"date": idx.strftime("%Y-%m-%d"), "price": round(float(row["Close"]), 2)}
                for idx, row in df.iterrows()
                if pd.notna(row.get("Close")) and idx.strftime("%Y-%m-%d") > last_date
            ]
            if yf_records:
                logger.info(f"Brent yfinance: supplemented {len(yf_records)} prices after {last_date}")
                return eia_records + yf_records
    except Exception as e:
        logger.warning(f"Brent yfinance failed: {e}")

    # Fallback: war-period prices from verified news sources (CNBC, Reuters)
    # EIA/FRED have a 2-4 day reporting lag; these fill the gap for the Iran war period
    reported_prices = [
        {"date": "2026-03-03", "price": 81.40},  # CNBC: Brent settles +4.71%
        {"date": "2026-03-04", "price": 82.76},  # Reuters: Brent +1.6%
        {"date": "2026-03-05", "price": 85.41},  # CNBC: Brent +4.93%, ~21% weekly
        {"date": "2026-03-06", "price": 87.12},  # Reuters: Brent +2.0%, analysts warn $100+
    ]
    supplement = [p for p in reported_prices if p["date"] > last_date]
    if supplement:
        logger.info(f"Brent: supplemented {len(supplement)} war-period prices from news sources (after {last_date})")
        return eia_records + supplement

    return eia_records


def _load_brent_csv_fallback() -> List[dict]:
    logger.info("Brent: falling back to CSV")
    if config.MYLES_DATASET_PATH.exists():
        df = pd.read_csv(config.MYLES_DATASET_PATH)
        date_col = df.columns[0]
        df.rename(columns={date_col: "Date"}, inplace=True)
        df["Date"] = pd.to_datetime(df["Date"])
        records = [
            {"date": row["Date"].strftime("%Y-%m-%d"), "price": float(row["Brent_Price"])}
            for _, row in df.iterrows()
            if pd.notna(row.get("Brent_Price"))
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
            timeout=30,
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

        records = [
            {"date": idx.strftime("%Y-%m-%d"), "value": round(float(row["Close"]), 4)}
            for idx, row in df.iterrows()
            if pd.notna(row.get("Close"))
        ]
        if records:
            _write_cache(cache_key, records)
            logger.info(f"yfinance {ticker}: fetched {len(records)} data points")
            return records
    except Exception as e:
        logger.warning(f"yfinance {ticker} failed: {e}")
    return []


def fetch_dxy() -> List[dict]:
    return fetch_yfinance_series("DX-Y.NYB", "dxy")


def fetch_ovx() -> List[dict]:
    return fetch_yfinance_series("^OVX", "ovx")


# ─── FRED API (China PMI) ───────────────────────────────────────────────────

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
            logger.info(f"FRED China PMI: fetched {len(records)} data points")
            return records
    except Exception as e:
        logger.warning(f"FRED API failed: {e}")
    return []


# ─── Master Dataset (CSV Backbone) ──────────────────────────────────────────

def load_master_dataset() -> dict:
    """Load the full myles_dataset_final.csv and return as structured JSON."""
    cached = _read_cache("master_dataset", 300)  # 5 min cache
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

    # Time series records
    timeseries = []
    for _, row in df.iterrows():
        timeseries.append({
            "date": row["Date"].strftime("%Y-%m-%d"),
            "brent_price": round(float(row["Brent_Price"]), 2) if pd.notna(row.get("Brent_Price")) else None,
            "daily_volatility": round(float(row["Daily_Volatility"]), 4) if pd.notna(row.get("Daily_Volatility")) else None,
            "weekly_attacks": int(row["WeekleyAttackFrq"]) if pd.notna(row.get("WeekleyAttackFrq")) else 0,
            "opec_dummy": int(row.get("OPEC_Dummy", 0)),
            "russia_ukraine_dummy": int(row.get("RussiaUkraine_Dummy", 0)),
            "opec_decision": int(row.get("OPEC_Decision", 0)),
            "russia_ukraine_attacks": int(row.get("RussiaUkraine_Attacks", 0)),
            "iran_israel_escalation": int(row.get("IranIsrael_Escalation", 0)),
            "china_pmi": round(float(row["China_PMI"]), 2) if pd.notna(row.get("China_PMI")) and row.get("China_PMI") != 0 else None,
            "baker_hughes_rigs": round(float(row["Baker_Hughes_Rigs"]), 1) if pd.notna(row.get("Baker_Hughes_Rigs")) and row.get("Baker_Hughes_Rigs") != 0 else None,
            "spr_release_volume": round(float(row["SPR_Release_Volume"]), 4) if pd.notna(row.get("SPR_Release_Volume")) else None,
            "dxy": round(float(row["DXY"]), 2) if pd.notna(row.get("DXY")) else None,
            "ovx": round(float(row["OVX"]), 2) if pd.notna(row.get("OVX")) else None,
        })

    # KPIs
    valid_prices = df["Brent_Price"].dropna()
    kpis = {
        "avg_brent_price": round(float(valid_prices.mean()), 2),
        "latest_brent_price": round(float(valid_prices.iloc[-1]), 2),
        "brent_price_change": round(float(valid_prices.iloc[-1] - valid_prices.iloc[-2]), 2) if len(valid_prices) > 1 else 0,
        "peak_volatility": round(float(df["Daily_Volatility"].max()), 4),
        "max_weekly_attacks": int(df["WeekleyAttackFrq"].max()),
        "latest_dxy": round(float(df["DXY"].dropna().iloc[-1]), 2) if df["DXY"].dropna().shape[0] > 0 else None,
        "latest_ovx": round(float(df["OVX"].dropna().iloc[-1]), 2) if df["OVX"].dropna().shape[0] > 0 else None,
        "total_trading_days": len(valid_prices),
    }

    # Price windows (event study: T-2 to T+5)
    price_cols = ["Price_T-2", "Price_T-1", "Price_T0", "Price_T+1", "Price_T+2", "Price_T+3", "Price_T+4", "Price_T+5"]
    attack_rows = df[df["WeekleyAttackFrq"] > 0]
    price_windows = {}
    for col in price_cols:
        if col in df.columns:
            values = attack_rows[col].replace(0, np.nan).dropna()
            price_windows[col] = round(float(values.mean()), 2) if len(values) > 0 else 0

    # Correlation matrix
    corr_cols = ["Brent_Price", "Daily_Volatility", "WeekleyAttackFrq", "DXY", "OVX",
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
        all_events = []

        # Query 1: Events in Iran
        for page in range(1, 20):
            resp = requests.get(
                config.ACLED_DATA_URL,
                headers={**_BROWSER_HEADERS, "Authorization": f"Bearer {token}"},
                params={
                    "_format": "json",
                    "country": "Iran",
                    "event_date": "2025-01-01|2026-12-31",
                    "event_date_where": "BETWEEN",
                    "fields": "event_id_cnty|event_date|event_type|sub_event_type|actor1|actor2|location|latitude|longitude|notes|fatalities|tags",
                    "limit": 5000,
                    "page": page,
                },
                timeout=60,
            )
            resp.raise_for_status()
            events = resp.json().get("data", [])
            if not events:
                break
            all_events.extend(events)
            logger.info(f"Iran events (country): fetched page {page} ({len(events)} events)")
            if len(events) < 5000:
                break

        # Query 2: US-Iran bilateral events globally (actor-based)
        seen_ids = {e.get("event_id_cnty") for e in all_events}
        for actor_pair in [
            {"actor1": "United States", "actor2": "Iran"},
            {"actor1": "Iran", "actor2": "United States"},
        ]:
            resp = requests.get(
                config.ACLED_DATA_URL,
                headers={**_BROWSER_HEADERS, "Authorization": f"Bearer {token}"},
                params={
                    "_format": "json",
                    "actor1": actor_pair["actor1"],
                    "actor1_where": "LIKE",
                    "actor2": actor_pair["actor2"],
                    "actor2_where": "LIKE",
                    "event_date": "2025-01-01|2026-12-31",
                    "event_date_where": "BETWEEN",
                    "fields": "event_id_cnty|event_date|event_type|sub_event_type|actor1|actor2|location|latitude|longitude|notes|fatalities|tags",
                    "limit": 5000,
                },
                timeout=60,
            )
            resp.raise_for_status()
            bilateral = resp.json().get("data", [])
            for e in bilateral:
                if e.get("event_id_cnty") not in seen_ids:
                    all_events.append(e)
                    seen_ids.add(e.get("event_id_cnty"))
            logger.info(f"Iran bilateral ({actor_pair['actor1']}→{actor_pair['actor2']}): {len(bilateral)} events")

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
        {"date": "2025-01-20", "title": "Trump Inaugurated — Rescinds Biden-Era Iran Policies", "type": "diplomatic", "description": "Trump signs EO 14148 rescinding 67 Biden-era executive orders including Iran sanctions-related actions.", "severity": 2, "lat": 38.9072, "lon": -77.0369, "location": "Washington, DC"},
        {"date": "2025-02-04", "title": "Trump Signs 'Maximum Pressure' Executive Order", "type": "sanctions", "description": "NSPM-2 restores maximum pressure campaign: Treasury to impose maximum economic pressure, State Dept rescinds sanctions waivers, campaign to drive Iran oil exports to zero.", "severity": 3, "lat": 38.9072, "lon": -77.0369, "location": "Washington, DC"},
        {"date": "2025-02-06", "title": "OFAC Sanctions Iranian Oil Shipping Network", "type": "sanctions", "description": "Treasury's OFAC sanctions international network of parties and vessels facilitating Iranian crude oil shipments to China.", "severity": 2, "lat": 38.9072, "lon": -77.0369, "location": "Washington, DC"},

        # ── Phase 2: Nuclear Talks Begin (Mar-Jun 2025) ──
        {"date": "2025-03-07", "title": "Trump Sends Letter to Khamenei with 2-Month Deadline", "type": "diplomatic", "description": "Trump sends letter via UAE diplomat Anwar Gargash proposing nuclear negotiations, warning of military consequences if rejected.", "severity": 3, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2025-04-12", "title": "Round 1: US-Iran Indirect Talks Begin in Muscat", "type": "diplomatic", "description": "First indirect US-Iran nuclear talks mediated by Oman. US envoy Witkoff and Iranian FM Araghchi in separate rooms. Both sides call talks 'constructive.'", "severity": 2, "lat": 23.5880, "lon": 58.3829, "location": "Muscat, Oman"},
        {"date": "2025-05-11", "title": "Round 4: US Demands Complete Dismantlement", "type": "diplomatic", "description": "Fourth round in Muscat. Witkoff demands complete dismantlement of Natanz, Fordow, and Isfahan. Positions harden.", "severity": 3, "lat": 23.5880, "lon": 58.3829, "location": "Muscat, Oman"},
        {"date": "2025-05-31", "title": "IAEA: Iran Has 400+ kg of 60% Enriched Uranium", "type": "nuclear", "description": "Confidential IAEA report confirms 400+ kg of 60% enriched uranium — enough for ~10 nuclear weapons if further enriched. Total stockpile 40x JCPOA limit.", "severity": 4, "lat": 33.5103, "lon": 51.9250, "location": "Natanz, Iran"},

        # ── Phase 3: The Twelve-Day War (Jun 2025) ──
        {"date": "2025-06-13", "title": "Israel Launches 'Operation Rising Lion' — Strikes Iran", "type": "military", "description": "Israel launches surprise strikes on Iranian nuclear facilities including Natanz. Prominent military leaders and nuclear scientists assassinated. US-Iran talks suspended.", "severity": 5, "lat": 33.5103, "lon": 51.9250, "location": "Natanz, Iran"},
        {"date": "2025-06-21", "title": "US Launches 'Operation Midnight Hammer'", "type": "military", "description": "125+ aircraft including seven B-2 bombers with GBU-57 bunker busters strike Fordow, Natanz, Isfahan. Tomahawks from submarines. Trump claims facilities 'obliterated.'", "severity": 5, "lat": 34.7564, "lon": 51.0596, "location": "Fordow, Iran"},
        {"date": "2025-06-22", "title": "Iran Retaliates — 550+ Missiles, 1000+ Drones at Israel", "type": "military", "description": "Iran launches over 550 ballistic missiles and 1,000+ drones at Israeli and US targets. Most intercepted by Israel and US.", "severity": 5, "lat": 32.0853, "lon": 34.7818, "location": "Tel Aviv, Israel"},
        {"date": "2025-06-24", "title": "Twelve-Day War Ceasefire Agreed", "type": "diplomatic", "description": "Israel and Iran agree to ceasefire under US pressure, ending the Twelve-Day War.", "severity": 4, "lat": 38.9072, "lon": -77.0369, "location": "Washington, DC"},

        # ── Phase 4: Snapback Sanctions & Isolation (Aug-Oct 2025) ──
        {"date": "2025-08-28", "title": "E3 Triggers JCPOA Snapback Sanctions Mechanism", "type": "sanctions", "description": "UK, France, and Germany invoke JCPOA snapback citing Iran's 'significant non-performance.' 30-day countdown begins.", "severity": 4, "lat": 40.7489, "lon": -73.9680, "location": "New York, NY (UN)"},
        {"date": "2025-09-27", "title": "UN Snapback Sanctions Formally Reimposed on Iran", "type": "sanctions", "description": "All UN sanctions lifted under JCPOA formally reimposed: travel bans, asset freezes, arms embargo, ballistic missile restrictions. EU follows Sept 29.", "severity": 4, "lat": 40.7489, "lon": -73.9680, "location": "New York, NY (UN)"},
        {"date": "2025-10-18", "title": "Iran Officially Terminates the JCPOA", "type": "diplomatic", "description": "Iran declares the JCPOA over on 'Termination Day.' Iran, Russia, and China declare UN sanctions invalid.", "severity": 3, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},

        # ── Phase 5: Protests & Crackdown (Dec 2025 - Jan 2026) ──
        {"date": "2025-12-28", "title": "Massive Anti-Regime Protests Erupt Across Iran", "type": "proxy", "description": "Protests erupt after rial collapses to 1.4M/$1. Tehran Grand Bazaar strikes spread nationwide — 72% food inflation, post-war devastation, snapback sanctions.", "severity": 4, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-01-08", "title": "Iran's Deadliest Crackdown Since 1979", "type": "military", "description": "Security forces launch massive crackdown. Internet fully cut. Firearms and shotguns with metal pellets used against protesters. Thousands reported killed.", "severity": 5, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-01-23", "title": "Trump Announces Naval 'Armada' Heading to Middle East", "type": "military", "description": "Trump announces USS Abraham Lincoln carrier strike group deployment. F/A-18E Super Hornets, F-35C Lightning IIs, guided-missile destroyers.", "severity": 4, "lat": 25.2854, "lon": 55.3500, "location": "Persian Gulf"},

        # ── Phase 6: War Buildup (Feb 2026) ──
        {"date": "2026-02-03", "title": "IRGC Attempts to Board US Tanker; Drone Shot Down", "type": "military", "description": "IRGC Navy attempts to intercept US-flagged tanker in Strait of Hormuz. USS McFaul escorts it to safety. F-35C shoots down Iranian Shahed-139 drone.", "severity": 3, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-02-06", "title": "Round 6: US-Iran Talks Resume in Muscat", "type": "diplomatic", "description": "First talks since June 2025. US delegation: Witkoff, Kushner, CENTCOM commander Adm. Cooper. Iranian FM Araghchi leads. 'Good start.'", "severity": 3, "lat": 23.5880, "lon": 58.3829, "location": "Muscat, Oman"},
        {"date": "2026-02-13", "title": "USS Gerald R. Ford Redeployed; Trump Signals Regime Change", "type": "military", "description": "Ford redirected to Middle East — largest US force posture since 2003 Iraq War. Trump says regime change would be 'best thing that could happen.'", "severity": 4, "lat": 25.2854, "lon": 55.3500, "location": "Persian Gulf"},
        {"date": "2026-02-14", "title": "Pentagon Prepares 'Weeks-Long Sustained Operations'", "type": "military", "description": "US officials confirm military is preparing for sustained operations against Iran lasting weeks.", "severity": 4, "lat": 38.8719, "lon": -77.0563, "location": "Pentagon, VA"},
        {"date": "2026-02-19", "title": "Trump Gives Iran 10-Day Ultimatum", "type": "diplomatic", "description": "Trump tells Iran to reach a 'meaningful' deal within 10-15 days or 'really bad things' will happen. IRGC conducts live-fire Strait of Hormuz drill.", "severity": 5, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-02-24", "title": "F-22s Deployed to Israel; State of the Union Warning", "type": "military", "description": "12 F-22s deployed to Ovda Airbase — first US offensive weapons in Israel. Trump vows in SOTU that Iran will never have nuclear weapons.", "severity": 4, "lat": 29.9402, "lon": 34.9358, "location": "Ovda Airbase, Israel"},
        {"date": "2026-02-26", "title": "Final Nuclear Talks Fail — No Deal Reached", "type": "diplomatic", "description": "Round 8 in Geneva. US demands: destroy all enrichment sites, surrender uranium, permanent deal, end proxies. Iran refuses missile restrictions. No agreement.", "severity": 4, "lat": 46.2044, "lon": 6.1432, "location": "Geneva, Switzerland"},
        {"date": "2026-02-27", "title": "IAEA Reveals Hidden Uranium; Embassies Evacuate Iran", "type": "nuclear", "description": "IAEA reports 440.9 kg of 60% enriched uranium hidden in Isfahan tunnels. Embassies evacuate. Trump gives go order for Operation Epic Fury from Air Force One.", "severity": 5, "lat": 32.6546, "lon": 51.6680, "location": "Isfahan, Iran"},

        # ── Phase 7: Operation Epic Fury / Iran War (Feb 28 - Mar 6, 2026) ──
        {"date": "2026-02-28", "title": "Operation Epic Fury Begins — US & Israel Strike Iran", "type": "military", "description": "Joint US-Israeli strikes at 2:30 AM EST. ~900 US strikes in 12 hours, 1,000+ targets in 24h. Israel's largest-ever air op: ~200 jets, ~500 targets. Tehran, Isfahan, Qom hit.", "severity": 5, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-02-28", "title": "Khamenei Assassinated in Israeli Airstrike", "type": "military", "description": "Supreme Leader Khamenei killed in Israeli strikes on Tehran compound using CIA intelligence. 40+ senior Iranian officials killed. Iran confirms death March 1.", "severity": 5, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-02-28", "title": "Iran Launches 'Operation True Promise IV' Retaliation", "type": "military", "description": "Iran fires dozens of ballistic missiles and drones at Israel and US bases across Jordan, Kuwait, Bahrain, Qatar, Iraq, Saudi Arabia, UAE. US Embassy in Kuwait hit.", "severity": 5, "lat": 32.0853, "lon": 34.7818, "location": "Tel Aviv, Israel"},
        {"date": "2026-03-01", "title": "Maersk Suspends Strait of Hormuz Transit", "type": "proxy", "description": "Maersk suspends all Strait of Hormuz crossings, reroutes around Cape of Good Hope. Tanker transits drop from 24/day to 4. Four US soldiers killed in Kuwait drone strike.", "severity": 5, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-03-02", "title": "IRGC Closes Strait of Hormuz; Hezbollah Enters War", "type": "military", "description": "IRGC officially closes Strait of Hormuz, threatens any ship that passes. 150+ ships anchored outside. Hezbollah fires rockets at Israel; IDF invades southern Lebanon.", "severity": 5, "lat": 26.5667, "lon": 56.2500, "location": "Strait of Hormuz"},
        {"date": "2026-03-03", "title": "Brent Surges to $81.40; Global Shipping Suspended", "type": "proxy", "description": "Brent settles at $81.40 (+4.71%). CMA CGM, Hapag-Lloyd, MSC suspend strait transits. Iranian drones hit Amazon data centers in Bahrain and UAE. Natanz explosion reported.", "severity": 5, "lat": 33.5103, "lon": 51.9250, "location": "Natanz, Iran"},
        {"date": "2026-03-04", "title": "Brent Hits $82.76; 1,100+ Iranian Civilians Killed", "type": "military", "description": "Brent rises to $82.76. US gas up 27 cents to $3.25/gal. Over 1,100 Iranian civilians killed since war began. Drone attacks near Iran-Azerbaijan border.", "severity": 5, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-03-05", "title": "Brent Surges to $85.41; Iran Fires 500+ Missiles, 2000 Drones", "type": "military", "description": "Brent at $85.41 (+4.93%, ~21% weekly). Insurance withdrawn for Hormuz transit. Iran has fired 500+ missiles and 2,000 drones. NATO intercepts missile over Turkey.", "severity": 5, "lat": 35.6892, "lon": 51.3890, "location": "Tehran, Iran"},
        {"date": "2026-03-06", "title": "Iran Targets Gulf States; Analysts Warn $100+ Oil", "type": "military", "description": "Iran strikes Saudi Arabia, Kuwait, Qatar, Bahrain, UAE. Missile hits Jerusalem. Bushehr Airport hit. Analysts warn Brent could hit $100-$120/bbl if disruptions persist.", "severity": 5, "lat": 28.9234, "lon": 50.8203, "location": "Bushehr, Iran"},
    ]


def compute_iran_impact(iran_events: list, brent_prices: list) -> dict:
    """Calculate oil price impact metrics around Iran events."""
    if not brent_prices:
        return {"kpis": {}, "impact_by_type": {}, "event_table": []}

    # Build price lookup by date
    price_map = {p["date"]: p["price"] for p in brent_prices}
    sorted_dates = sorted(price_map.keys())

    def get_price_at_offset(date_str: str, offset: int):
        """Get price at T+offset trading days from date."""
        try:
            idx = sorted_dates.index(date_str)
            target_idx = idx + offset
            if 0 <= target_idx < len(sorted_dates):
                return price_map[sorted_dates[target_idx]]
        except (ValueError, IndexError):
            pass
        # Fallback: find nearest date
        try:
            from datetime import datetime, timedelta
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            for delta in range(0, 10):
                check = (dt + timedelta(days=offset + delta)).strftime("%Y-%m-%d")
                if check in price_map:
                    return price_map[check]
                check = (dt + timedelta(days=offset - delta)).strftime("%Y-%m-%d")
                if check in price_map:
                    return price_map[check]
        except Exception:
            pass
        return None

    # Get curated events for impact table
    curated = get_curated_iran_events()

    # Build impact table from curated events
    event_table = []
    for ev in curated:
        d = ev["date"]
        price_before = get_price_at_offset(d, -1)
        price_after = get_price_at_offset(d, 3)
        price_day = get_price_at_offset(d, 0)

        change_pct = None
        if price_before and price_after:
            change_pct = round((price_after - price_before) / price_before * 100, 2)

        event_table.append({
            "date": d,
            "title": ev["title"],
            "type": ev["type"],
            "severity": ev["severity"],
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
                pb = get_price_at_offset(d, -1)
                pa = get_price_at_offset(d, off)
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
        pb = get_price_at_offset(d, -1)
        pa = get_price_at_offset(d, 3)
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
    """Return hypothesis test results from the econometric analysis."""
    return {
        "h1": {
            "name": "H1: Attack Frequency",
            "description": "Higher frequency of Houthi maritime attacks increases Brent crude oil price volatility",
            "coefficient": -0.1029,
            "p_value": 0.0000,
            "r_squared": 0.1188,
            "supported": False,
            "conclusion": "NOT SUPPORTED. The coefficient is statistically significant but negative, suggesting that as attack frequency increased, the market adapted and volatility actually decreased — consistent with a 'new normal' effect."
        },
        "h2": {
            "name": "H2: Tanker Specificity",
            "description": "Attacks specifically targeting oil tankers have a greater impact on volatility than general maritime attacks",
            "coefficient": -0.1496,
            "p_value": 0.0201,
            "r_squared": 0.0056,
            "supported": False,
            "conclusion": "NOT SUPPORTED. While statistically significant (p=0.020), the negative coefficient and very low R² indicate tanker-specific attacks do not amplify volatility beyond the general attack effect."
        },
        "h3": {
            "name": "H3: Chokepoint Geography",
            "description": "Attacks at the Bab el-Mandeb strait chokepoint have a disproportionate impact on oil price volatility",
            "coefficient": -0.0017,
            "p_value": 0.8329,
            "r_squared": 0.0001,
            "supported": False,
            "conclusion": "NOT SUPPORTED. The coefficient is neither statistically significant (p=0.833) nor economically meaningful (R²≈0), indicating chokepoint proximity alone does not drive differential market response."
        },
        "garch_summary": {
            "model": "GJR-GARCH(1,1,1)",
            "distribution": "Normal",
            "mean_model": "Constant",
            "observations": 731,
            "log_likelihood": -1185.2,
            "aic": 2374,
            "bic": 2384,
        },
        "model_comparison": {
            "labels": ["H1 (Generic)", "H2 (Tanker)", "H3 (Chokepoint)"],
            "r_squared": [0.11883, 0.00564, 0.00006],
            "finding": "The market reacts most strongly to raw VOLUME of attacks (H1), but in the opposite direction expected — higher attack frequency correlates with lower volatility over time."
        }
    }
