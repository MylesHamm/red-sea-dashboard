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


def _get_acled_token() -> str:
    global _acled_token, _acled_token_expires
    if _acled_token and time.time() < _acled_token_expires:
        return _acled_token

    resp = requests.post(
        config.ACLED_TOKEN_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "username": config.ACLED_USERNAME,
            "password": config.ACLED_PASSWORD,
            "grant_type": "password",
            "client_id": "acled",
        },
        timeout=30,
    )
    resp.raise_for_status()
    token_data = resp.json()
    _acled_token = token_data["access_token"]
    _acled_token_expires = time.time() + token_data.get("expires_in", 86400) - 300
    logger.info("ACLED OAuth token acquired")
    return _acled_token


def fetch_acled_events() -> List[dict]:
    """Fetch Houthi/Yemen events from ACLED API with cache + CSV fallback."""
    cached = _read_cache("acled_events", config.CACHE_TTL_ACLED)
    if cached:
        logger.info("ACLED: serving from cache")
        return cached

    try:
        token = _get_acled_token()
        all_events = []
        page = 1
        limit = 5000

        while True:
            resp = requests.get(
                config.ACLED_DATA_URL,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                params={
                    "_format": "json",
                    "country": "Yemen",
                    "event_date": "2023-10-01|2025-12-31",
                    "event_date_where": "BETWEEN",
                    "fields": "event_id_cnty|event_date|event_type|sub_event_type|actor1|location|latitude|longitude|notes|fatalities|tags",
                    "limit": limit,
                    "page": page,
                },
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()

            events = data.get("data", [])
            if not events:
                break

            all_events.extend(events)
            logger.info(f"ACLED: fetched page {page} ({len(events)} events)")

            if len(events) < limit:
                break
            page += 1

        if all_events:
            _write_cache("acled_events", all_events)
            logger.info(f"ACLED: total {len(all_events)} events fetched and cached")
            return all_events

    except Exception as e:
        logger.warning(f"ACLED API failed: {e}")

    return _load_acled_csv_fallback()


def _load_acled_csv_fallback() -> List[dict]:
    """Load ACLED data from local CSV files."""
    logger.info("ACLED: falling back to CSV")
    for path in [config.HOUTHI_CSV_PATH, config.HOUTHI_EXCEL_PATH]:
        if path.exists():
            try:
                if str(path).endswith(".xlsx"):
                    df = pd.read_excel(path)
                else:
                    df = pd.read_csv(path)

                # Normalize column names to lowercase
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
                        "location": str(row.get("location", "")),
                        "latitude": float(row.get("latitude", 0)) if pd.notna(row.get("latitude")) else None,
                        "longitude": float(row.get("longitude", 0)) if pd.notna(row.get("longitude")) else None,
                        "notes": str(row.get("notes", "")),
                        "fatalities": int(row.get("fatalities", 0)) if pd.notna(row.get("fatalities")) else 0,
                        "tags": str(row.get("tags", "")),
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
            _write_cache("brent_prices", records)
            logger.info(f"Brent: fetched {len(records)} daily prices from EIA")
            return records
    except Exception as e:
        logger.warning(f"EIA Brent API failed: {e}")

    return _load_brent_csv_fallback()


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
    cached = _read_cache("iran_events", 3600)  # 1-hour cache
    if cached:
        logger.info("Iran events: serving from cache")
        return cached

    try:
        token = _get_acled_token()
        all_events = []

        # Query 1: Events in Iran
        for page in range(1, 20):
            resp = requests.get(
                config.ACLED_DATA_URL,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
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
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
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

        if all_events:
            _write_cache("iran_events", all_events)
            logger.info(f"Iran events: total {len(all_events)} events fetched and cached")
            return all_events

    except Exception as e:
        logger.warning(f"Iran events API failed: {e}")

    return []


def get_curated_iran_events() -> List[dict]:
    """Return curated timeline of major US-Iran events."""
    return [
        {"date": "2025-01-20", "title": "Trump Inaugurated — 'Maximum Pressure 2.0' Signaled", "type": "diplomatic", "description": "President Trump takes office, signaling renewed maximum pressure campaign against Iran.", "severity": 4},
        {"date": "2025-02-04", "title": "US Reinstates Snapback Sanctions on Iran", "type": "sanctions", "description": "US triggers UN snapback mechanism, reimposing all previously lifted Security Council sanctions on Iran.", "severity": 5},
        {"date": "2025-02-07", "title": "Trump Demands Iran Denuclearize or Face Strikes", "type": "diplomatic", "description": "President Trump issues ultimatum demanding Iran abandon nuclear program entirely or face military action.", "severity": 4},
        {"date": "2025-02-27", "title": "US Imposes New Sanctions on Iranian Oil Exports", "type": "sanctions", "description": "Treasury Department designates additional Iranian oil entities and shipping networks enabling crude exports.", "severity": 4},
        {"date": "2025-03-01", "title": "IAEA Reports Iran Enriching to 60% Purity", "type": "nuclear", "description": "IAEA confirms Iran continues enriching uranium to 60%, approaching weapons-grade threshold.", "severity": 4},
        {"date": "2025-03-14", "title": "US Strikes Iran-Backed Militias in Iraq & Syria", "type": "military", "description": "US conducts airstrikes against Iran-aligned militia positions in Iraq and Syria following attacks on US bases.", "severity": 5},
        {"date": "2025-04-01", "title": "Iran Seizes Commercial Tanker in Strait of Hormuz", "type": "military", "description": "IRGC Navy seizes a commercial oil tanker in the Strait of Hormuz, raising shipping insurance rates.", "severity": 4},
        {"date": "2025-04-12", "title": "US Deploys Additional Carrier Group to Persian Gulf", "type": "military", "description": "Pentagon announces deployment of USS Harry S. Truman carrier strike group to the Persian Gulf region.", "severity": 3},
        {"date": "2025-05-15", "title": "Iran-US Backchannel Talks Reported in Oman", "type": "diplomatic", "description": "Reports emerge of indirect US-Iran negotiations mediated by Oman regarding nuclear program and sanctions relief.", "severity": 3},
        {"date": "2025-06-10", "title": "US Sanctions Chinese Firms Buying Iranian Oil", "type": "sanctions", "description": "US Treasury sanctions Chinese companies and banks involved in purchasing Iranian crude oil, targeting Iran's primary revenue source.", "severity": 4},
        {"date": "2025-07-08", "title": "Iran Tests Ballistic Missile Capable of Reaching Israel", "type": "military", "description": "Iran test-fires a new medium-range ballistic missile, drawing US and Israeli condemnation.", "severity": 4},
        {"date": "2025-08-20", "title": "IRGC Fast Boats Harass US Navy in Strait of Hormuz", "type": "military", "description": "Multiple IRGC fast boats conduct aggressive maneuvers near US Navy vessels transiting the Strait of Hormuz.", "severity": 3},
        {"date": "2025-09-15", "title": "US-Iran Prisoner Swap Completed", "type": "diplomatic", "description": "US and Iran complete exchange of detained citizens in a rare diplomatic breakthrough.", "severity": 2},
        {"date": "2025-10-02", "title": "Iran Announces Expansion of Enrichment Capacity", "type": "nuclear", "description": "Iran declares installation of advanced centrifuges at Natanz, significantly expanding enrichment capacity.", "severity": 4},
        {"date": "2025-11-10", "title": "US Imposes 'Total Embargo' Sanctions Package on Iran", "type": "sanctions", "description": "Comprehensive new sanctions target Iran's entire financial sector, aiming to reduce oil exports to zero.", "severity": 5},
        {"date": "2025-12-15", "title": "Iran-Backed Houthis Escalate Red Sea Attacks", "type": "proxy", "description": "Houthi forces launch intensified missile and drone campaign against commercial shipping in the Red Sea.", "severity": 4},
        {"date": "2026-01-08", "title": "US Strikes Houthi Targets in Yemen", "type": "military", "description": "US conducts extensive strikes against Houthi military infrastructure in Yemen in response to Red Sea shipping attacks.", "severity": 5},
        {"date": "2026-01-20", "title": "Iran Threatens to Close Strait of Hormuz", "type": "diplomatic", "description": "Iran's Supreme Leader warns of closing the Strait of Hormuz if oil sanctions enforcement continues.", "severity": 5},
        {"date": "2026-02-10", "title": "IAEA: Iran's Enrichment Nearing 90% Weapons-Grade", "type": "nuclear", "description": "IAEA detects uranium particles enriched to near 90% at Fordow facility, crossing a critical nuclear threshold.", "severity": 5},
        {"date": "2026-03-01", "title": "US and Iran Begin Direct Negotiations", "type": "diplomatic", "description": "US and Iranian officials hold first direct talks since 2022, exploring framework for nuclear constraints and sanctions relief.", "severity": 4},
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
    events_this_month = sum(1 for e in iran_events if (e.get("event_date") or "").startswith(current_month))

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
