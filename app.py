"""
Red Sea Crisis Intelligence Dashboard - FastAPI Backend
Run: python app.py
"""
import asyncio
import json
import threading
import time
from functools import partial

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.background import BackgroundTasks
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.gzip import GZipMiddleware

import config
import data_service
# ais_service.py was the original live-AIS WebSocket worker that powered
# the "VESSELS IN KILL ZONE" sidebar. That sidebar was replaced with
# /api/chokepoint-incidents (real ACLED + curated events) in early 2026
# because AISStream's free tier rate-limits a single concurrent socket
# per key, which couldn't sustain three chokepoint feeds at once. The
# module was removed during the dead-code strip.


def _run_sync(fn, *args):
    """Run a blocking function in the default executor."""
    loop = asyncio.get_running_loop()
    return loop.run_in_executor(None, fn if not args else partial(fn, *args))

app = FastAPI(title="Middle East Energy Security Dashboard")

# Gzip responses over 500 bytes. The big win here is /api/events (~13MB JSON
# of 17k ACLED events) compressing to ~2MB over the wire — the single largest
# factor in Geospatial/Incidents tab load time. Gzip is ~negligible CPU at
# compresslevel=5 even for large responses.
app.add_middleware(GZipMiddleware, minimum_size=500, compresslevel=5)

# Serve static files (HTML, CSS, JS) with no-cache headers

class NoCacheStaticMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/assets") or request.url.path == "/":
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

app.add_middleware(NoCacheStaticMiddleware)
# Serve /assets/* (JS, CSS) directly from the project's assets/ folder so the
# same dashboard.html works locally (file://) and in production.
app.mount("/assets", StaticFiles(directory=config.BASE_DIR / "assets"), name="assets")


# ─── API Endpoints ───────────────────────────────────────────────────────────

@app.get("/api/health")
async def health_check():
    """Fast health check for Render — does not trigger data loading."""
    return {"status": "ok"}


@app.get("/api/constants")
async def get_constants():
    """Single source of truth for domain constants used by the frontend.

    Replaces the per-file hardcoded numbers (RADIUS_KM, GLOBAL_LIQUIDS_MBD,
    chokepoint reference flows, war-onset date, etc.) that used to be
    scattered across hydrate.js, charts.js, app.js, data.js. The frontend
    bootstraps from this once, exposes via window.CONSTANTS, then every
    component reads the value from there. Changing a number in config.py
    propagates to every consumer on the next page load.
    """
    return {
        "anchors": {
            "hamas_attack":  config.HAMAS_ATTACK_DATE,
            "houthi_phase":  config.HOUTHI_PHASE_START,
            "war_onset":     config.WAR_ONSET_DATE,
        },
        "thesis_window": {
            "start": config.THESIS_WINDOW_START,
            "end":   config.THESIS_WINDOW_END,
            "n":     config.THESIS_WINDOW_N,
        },
        "chokepoint_flow_mbd":   config.CHOKEPOINT_REF_FLOW_MBD,
        "global_liquids_mbd":    config.GLOBAL_LIQUIDS_MBD,
        "chokepoint_radius_km":  config.CHOKEPOINT_RADIUS_KM,
        "incident_bounding_boxes": {
            cp: {"top_left": list(box[0]), "bottom_right": list(box[1])}
            for cp, box in config.INCIDENT_BOUNDING_BOXES.items()
        },
        "incident_actor_hints": {k: list(v) for k, v in config.INCIDENT_ACTOR_HINTS.items()},
        "incident_keywords":    {k: list(v) for k, v in config.INCIDENT_KEYWORDS.items()},
        "threat_tiers":         [{"name": n, "min_decline_pct": p} for n, p in config.THREAT_TIERS],
    }


@app.get("/api/dashboard-state")
async def get_dashboard_state():
    """Composed dashboard state — every derived KPI in one place.

    Replaces the pattern where the frontend computed `incidents30d`,
    `oilFlowAtRisk`, `warPremiumPct`, etc. from raw API responses in
    multiple components, leading to multi-writer races (the "59 → 32
    flicker" we just fixed). The values are computed server-side once
    per request — frontend reads them directly.
    """
    import time as _time
    from datetime import datetime as _dt, timezone as _tz

    # ── Source feeds (read memos / caches; never trigger a fetch) ───
    master   = await _run_sync(data_service.load_master_dataset)
    iran_resp_curated = data_service.get_merged_curated_events(include_news=True) or []
    iran_news_cache = data_service._read_cache("iran_news", 7200) or []

    ts = (master or {}).get("timeseries") or []
    kpis_master = (master or {}).get("kpis") or {}

    # ── Brent KPIs (latest, delta, war premium, 30D range) ──────────
    brent_rows = [r for r in ts if r.get("brent_price") is not None]
    latest = brent_rows[-1] if brent_rows else None
    prior  = brent_rows[-2] if len(brent_rows) >= 2 else None
    war_anchor = next((r for r in brent_rows if r.get("date", "") >= config.WAR_ONSET_DATE), None)
    last30 = brent_rows[-30:] if len(brent_rows) >= 2 else brent_rows

    def _pct(cur, base):
        if cur is None or base in (None, 0): return None
        return round((cur - base) / base * 100, 2)

    # Prefer FMP intraday for the live price; fall back to EIA daily settle.
    # FMP gives minute-fresh BZUSD vs EIA's once-a-day prior-session close —
    # closes the "no intraday data" gap that limited client-grade use.
    fmp_q = data_service.fetch_fmp_brent_quote() if config.FMP_API_KEY else {}
    fmp_price       = fmp_q.get("price")
    fmp_change      = fmp_q.get("change")
    fmp_change_pct  = fmp_q.get("change_pct")
    has_intraday    = fmp_price is not None
    eia_price       = (latest or {}).get("brent_price")
    eia_prior       = (prior  or {}).get("brent_price")

    brent_kpi = {
        # Live price: FMP intraday if available, else EIA daily settle.
        "price":          fmp_price if has_intraday else eia_price,
        "change_24h":     round(fmp_change, 2) if fmp_change is not None
                          else (round(eia_price - eia_prior, 2) if eia_price is not None and eia_prior else None),
        "change_24h_pct": round(fmp_change_pct, 2) if fmp_change_pct is not None
                          else _pct(eia_price, eia_prior),
        "day_low":        fmp_q.get("day_low") if has_intraday else None,
        "day_high":       fmp_q.get("day_high") if has_intraday else None,
        "range_30d":      [
            min((r.get("brent_price") for r in last30 if r.get("brent_price") is not None), default=None),
            max((r.get("brent_price") for r in last30 if r.get("brent_price") is not None), default=None),
        ],
        "war_premium_pct": _pct(fmp_price if has_intraday else eia_price, (war_anchor or {}).get("brent_price")) if war_anchor else None,
        "as_of":          fmp_q.get("timestamp") if has_intraday else (latest or {}).get("date"),
        "source":         "FMP intraday (BZUSD)" if has_intraday else "EIA daily settle",
        "is_intraday":    has_intraday,
    }

    # ── OVX / DXY (latest from master timeseries) ───────────────────
    def _last_nonnull(key):
        for r in reversed(ts):
            if r.get(key) is not None:
                return r.get(key), r.get("date")
        return None, None
    ovx_v, ovx_d = _last_nonnull("ovx")
    dxy_v, dxy_d = _last_nonnull("dxy")

    # ── INCIDENTS · 30D — globally deduped union of curated + news ──
    # Same logic the frontend's refreshHeroIncidentsKpi was doing,
    # moved to the backend so it's the canonical answer.
    now_ts = _time.time()
    cutoff30 = now_ts - 30 * 86400
    cutoff60 = now_ts - 60 * 86400
    seen = set()
    inc_30d = inc_prior = 0
    def _accept(date, title):
        nonlocal inc_30d, inc_prior
        if not date: return
        key = f"{date[:10]}::{(title or '').strip().lower()[:50]}"
        if key in seen: return
        seen.add(key)
        try:
            t = _time.mktime(_time.strptime(date[:10], "%Y-%m-%d"))
        except Exception:
            return
        if t >= cutoff30:      inc_30d += 1
        elif t >= cutoff60:    inc_prior += 1
    for c in iran_resp_curated:
        _accept(c.get("date"), c.get("title") or c.get("label"))
    for n in iran_news_cache:
        _accept(n.get("date"), n.get("title") or n.get("label"))

    # ── Weekly oil-events series — for §01 "Price vs Conflict Intensity" ───
    # Sourced from the UNION of the Hormuz + Bab chokepoint pools (the
    # chart's title is "BAB + HORMUZ THEATRES"). Both pools share the same
    # selection logic (curated war timeline + iran_news + ACLED, keyword-
    # filtered to that chokepoint) so this is the canonical "weekly conflict
    # events for the threatened chokepoints" series. Bucketed by ISO week.
    #
    # Earlier versions sourced from HDX country-level Yemen + Iran totals
    # (~600/week, included civil unrest) — same axis label, totally different
    # scope from the cards. User flagged the inconsistency.
    from collections import defaultdict as _dd
    from datetime import timedelta as _td
    weekly_buckets: _dd = _dd(int)
    seen_w = set()
    def _bucket_event(date, key_id):
        if not date: return
        if key_id in seen_w: return
        seen_w.add(key_id)
        try:
            d = _dt.strptime(date[:10], "%Y-%m-%d")
            monday = d - _td(days=d.weekday())
            weekly_buckets[monday.date().isoformat()] += 1
        except Exception:
            return
    for cp_id in ("hormuz", "bab"):
        try:
            cp_pool = data_service.get_chokepoint_incidents(cp_id, 540, 5000)
        except Exception:
            cp_pool = {"events": []}
        for ev in cp_pool.get("events", []):
            # Stable dedup key — prefer event_id (curated/news entries carry
            # one); fall back to date+title-prefix.
            eid = ev.get("event_id") or f"{ev.get('date','')[:10]}::{(ev.get('actor1') or '').strip().lower()[:50]}"
            _bucket_event(ev.get("date"), eid)
    weekly_oil_events = [{"week_start": k, "count": v} for k, v in sorted(weekly_buckets.items())]

    # ── OIL FLOW AT RISK — Hormuz + Bab structural (mbd) ────────────
    flow_h = config.CHOKEPOINT_REF_FLOW_MBD.get("hormuz", 21.0)
    flow_b = config.CHOKEPOINT_REF_FLOW_MBD.get("bab",     8.2)
    flow_total = flow_h + flow_b

    # ── Per-chokepoint snapshot ─────────────────────────────────────
    chokepoints = {}
    for cp_id in ("hormuz", "bab", "suez"):
        cp_data = data_service.get_chokepoint_incidents(cp_id, 90, 200)
        events = cp_data.get("events", [])
        ev_30d = sum(1 for e in events if (e.get("ts") or 0) >= cutoff30)
        chokepoints[cp_id] = {
            "ref_flow_mbd": config.CHOKEPOINT_REF_FLOW_MBD.get(cp_id),
            "incidents_30d": ev_30d,
            "incidents_90d": len(events),
            "zone_newest_date": cp_data.get("zone_newest_date"),
            "wall_clock_cutoff": cp_data.get("wall_clock_cutoff"),
        }

    # ── Energy equity tape (XOM / CVX / OXY) — oil-impact context ───
    # Major-integrated oil stocks move with crude price expectations and
    # add a market-pricing signal alongside the futures tape. Free-tier
    # FMP handles three quotes per refresh well within the daily budget.
    energy_equities = []
    if config.FMP_API_KEY:
        for sym in ("XOM", "CVX", "OXY"):
            q = data_service.fetch_fmp_equity_quote(sym)
            if q.get("price") is not None:
                energy_equities.append(q)

    # ── Wall-clock date string for UI captions ──────────────────────
    today_iso = _dt.now(_tz.utc).date().isoformat()

    return {
        "as_of":  today_iso,
        "kpis": {
            "brent": brent_kpi,
            "ovx":   {"value": ovx_v, "as_of": ovx_d},
            "dxy":   {"value": dxy_v, "as_of": dxy_d},
            "incidents_30d": {
                "count":      inc_30d,
                "prior_30d":  inc_prior,
                "delta":      inc_30d - inc_prior,
                "as_of":      today_iso,
            },
            "oil_flow_at_risk": {
                "mbd":        round(flow_total, 1),
                "pct_global": round(flow_total / config.GLOBAL_LIQUIDS_MBD * 100, 1),
            },
        },
        "chokepoints": chokepoints,
        "energy_equities": energy_equities,
        "weekly_oil_events": weekly_oil_events,
        "anchors": {
            "war_onset":    config.WAR_ONSET_DATE,
            "hamas_attack": config.HAMAS_ATTACK_DATE,
        },
    }


@app.get("/api/diag")
async def diagnostic():
    """Diagnostic snapshot: data freshness + last fetch errors.

    Safe to expose publicly — does not leak credentials, only boolean flags.
    Use to verify Render env vars are set and surface silent ACLED failures.
    """
    from datetime import datetime, timezone

    def _date_range(events, key):
        dates = sorted(e.get(key, "") for e in events if e.get(key))
        return {"count": len(events), "min": dates[0] if dates else None, "max": dates[-1] if dates else None}

    acled_meta = data_service.get_acled_fetch_meta()
    # Read memos only — never trigger a fetch here.
    acled_events = data_service._acled_events_memo or []
    iran_events = data_service._iran_fallback_memo or []

    def _iso(ts):
        if not ts:
            return None
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

    # Token state: useful when last_fetch_error mentions auth.
    token_present = bool(getattr(data_service, "_acled_token", None))
    token_exp = getattr(data_service, "_acled_token_expires", 0)
    token_exp_iso = _iso(token_exp) if token_exp else None
    token_exp_in_s = max(0, int(token_exp - data_service.time.time())) if token_exp else None

    # On-disk cache file freshness — distinguishes "memo serving stale data"
    # from "no cache file at all".
    import os as _os
    cache_path = data_service.config.CACHE_DIR / "acled_events.json"
    cache_file_age_s = None
    if cache_path.exists():
        try:
            cache_file_age_s = int(data_service.time.time() - _os.path.getmtime(cache_path))
        except Exception:
            pass

    return {
        "server_time_utc": datetime.now(timezone.utc).isoformat(),
        "acled": {
            "credentials_configured": acled_meta["credentials_configured"],
            "last_fetch_source": acled_meta["source"],
            "last_fetch_utc": _iso(acled_meta["ts"]),
            "last_fetch_error": acled_meta["error"],
            "token_present": token_present,
            "token_expires_utc": token_exp_iso,
            "token_expires_in_s": token_exp_in_s,
            "cache_file_age_s": cache_file_age_s,
            "query_date_range": data_service._ACLED_DATE_RANGE,
            **_date_range(acled_events, "event_date"),
        },
        "iran_events": {
            "last_fetch_error": data_service.get_iran_fetch_error(),
            **_date_range(iran_events, "event_date"),
        },
    }


@app.get("/api/master")
async def get_master_data():
    """Full master dataset: timeseries, KPIs, price windows, correlation."""
    return await _run_sync(data_service.load_master_dataset)


# Fields dropped from the LITE /api/events response. These are the heavy
# string fields the dashboard doesn't need for maps / KPIs / Houthi filters:
#   notes        ~ 300–900 chars/event — ACLED prose summary (only used by the
#                  Data Explorer's full-text search)
#   source       ~ 40–120 chars/event — news-source credit
#   source_scale ~ 15–30 chars/event — "Regional / National / ..."
#   tags         ~ 20–80 chars/event — ACLED internal taxonomy tags
# Dropping these cuts the payload roughly 60–70% (~13MB → ~4–5MB) and more
# than doubles front-end parse speed on Render's slow egress path. The Data
# Explorer can opt back in via /api/events?lite=0 when the user starts
# typing a search query.
_EVENT_LITE_DROP = ("notes", "source", "source_scale", "tags")


def _lite_event(e: dict) -> dict:
    # New dict (don't mutate the cached one — the full version is shared by
    # both the diag endpoint and the in-process memo).
    return {k: v for k, v in e.items() if k not in _EVENT_LITE_DROP}


@app.get("/api/events")
async def get_events(lite: int = 1):
    """ACLED event data for map and table.

    Defaults to the LITE payload (drops `notes`/`source`/`source_scale`/`tags`)
    to keep the Incidents tab responsive on slow networks. Pass `?lite=0` to
    get the full payload with prose `notes` — used by the Data Explorer's
    search box on focus.
    """
    events = await _run_sync(data_service.fetch_acled_events)
    if lite:
        events = [_lite_event(e) for e in events]
    return {"count": len(events), "data": events, "lite": bool(lite)}


@app.get("/api/thesis-events")
async def get_thesis_events():
    """The 726 ACLED-verified maritime events analyzed in the thesis."""
    events = await _run_sync(data_service.load_thesis_events)
    return {"count": len(events), "data": events}


@app.get("/api/brent")
async def get_brent():
    """Live Brent crude prices from EIA API."""
    prices = await _run_sync(data_service.fetch_brent_prices)
    return {"count": len(prices), "data": prices}


@app.get("/api/dxy")
async def get_dxy():
    """US Dollar Index from yfinance."""
    data = await _run_sync(data_service.fetch_dxy)
    return {"count": len(data), "data": data}


@app.get("/api/ovx")
async def get_ovx():
    """Oil Volatility Index from yfinance."""
    data = await _run_sync(data_service.fetch_ovx)
    return {"count": len(data), "data": data}


@app.get("/api/spr")
async def get_spr():
    """Strategic Petroleum Reserve data from EIA."""
    data = await _run_sync(data_service.fetch_spr_data)
    return {"count": len(data), "data": data}


@app.get("/api/china-pmi")
async def get_china_pmi():
    """China business confidence from FRED."""
    data = await _run_sync(data_service.fetch_china_pmi)
    return {"count": len(data), "data": data}


@app.get("/api/hypothesis")
async def get_hypothesis():
    """Hypothesis test results from econometric analysis."""
    return data_service.get_hypothesis_results()


@app.get("/api/suez-transits")
async def get_suez_transits():
    """Monthly Suez Canal transit volumes from IMF PortWatch."""
    data = await _run_sync(data_service.fetch_suez_transits)
    return {"count": len(data), "data": data}


@app.get("/api/bab-el-mandeb-transits")
async def get_bab_el_mandeb_transits():
    """Monthly Bab el-Mandeb Strait transit volumes from IMF PortWatch."""
    data = await _run_sync(data_service.fetch_bab_el_mandeb_transits)
    return {"count": len(data), "data": data}


@app.get("/api/hormuz-transits")
async def get_hormuz_transits():
    """Monthly Strait of Hormuz transit volumes from IMF PortWatch."""
    data = await _run_sync(data_service.fetch_hormuz_transits)
    return {"count": len(data), "data": data}


@app.get("/api/iran-events")
async def get_iran_events():
    """Iran-related ACLED events + curated major events + live news.
    Serves cached/fallback data immediately; ACLED fetch happens in background."""
    # Try cache first (instant), then JSON fallback (instant)
    acled_events = data_service._read_cache("iran_events", 3600)
    if not acled_events:
        acled_events = data_service._load_iran_json_fallback()
    # If still empty, do a live fetch (slow path — only on first cold start)
    if not acled_events:
        acled_events = await _run_sync(data_service.fetch_iran_events)

    # News: use cache if available, otherwise fetch (but don't block on stale cache)
    news = data_service._read_cache("iran_news", 7200) or []
    if not news:
        # Cold start: fetch synchronously so first visitor gets news
        news = await _run_sync(data_service.fetch_iran_news)

    curated = data_service.get_merged_curated_events()
    result = {"count": len(acled_events), "data": acled_events, "curated": curated, "news": news}
    err = data_service.get_iran_fetch_error()
    if err:
        result["error"] = err
    return result


@app.get("/api/iran-impact")
async def get_iran_impact():
    """Oil price impact analysis around Iran events.
    Serves cached/fallback data immediately.

    Filters out civilian protests / riots before computing impact KPIs —
    these don't move oil markets and were dragging the avg/peak price-
    move calculations toward zero (most protests = no measurable price
    move, biasing the average downward). Same oil-relevance filter
    applied throughout the dashboard.
    """
    # Try cache first, then JSON fallback
    iran_events = data_service._read_cache("iran_events", 3600)
    if not iran_events:
        iran_events = data_service._load_iran_json_fallback()
    if not iran_events:
        iran_events = await _run_sync(data_service.fetch_iran_events)
    # Apply oil-relevance filter
    if iran_events:
        iran_events = [
            e for e in iran_events
            if (e.get("event_type") or "").strip().lower() not in ("protests", "riots")
        ]

    # Brent: use cache first, then fetch
    brent_prices = data_service._read_cache("brent_prices", 3600)
    if not brent_prices:
        brent_prices = await _run_sync(data_service.fetch_brent_prices)

    result = data_service.compute_iran_impact(iran_events, brent_prices)
    result["brent_prices"] = brent_prices
    return result


# ─── Comparative Analysis & Iran Intensity ─────────────────────────────────────

@app.get("/api/comparative")
async def get_comparative():
    """Houthi vs Iran comparative analysis dataset."""
    return await _run_sync(data_service.get_comparative_data)


@app.get("/api/iran-intensity")
async def get_iran_intensity():
    """Iran conflict intensity time series."""
    return await _run_sync(data_service.compute_iran_intensity)


@app.get("/api/hormuz-status")
async def get_hormuz_status():
    """Hormuz disruption tracker."""
    return await _run_sync(data_service.compute_hormuz_disruption)


@app.get("/api/war-phases")
async def get_war_phases():
    """War phase definitions for timeline annotations."""
    return data_service.get_war_phases()


@app.get("/api/iran-regression")
async def get_iran_regression():
    """Run OLS regression on Iran war data."""
    return await _run_sync(data_service.run_iran_regression)


# ─── Tier-1 Free APIs (EIA inventories, FRED macro, GDELT tone) ─────────────

@app.get("/api/eia-inventories")
async def get_eia_inventories():
    """Weekly US commercial crude inventories + Cushing hub stocks (EIA v2)."""
    return await _run_sync(data_service.fetch_eia_inventories)


@app.get("/api/macro-context")
async def get_macro_context():
    """Macro context bundle from FRED: WTI, 5y breakeven, VIX, HY yield."""
    return await _run_sync(data_service.fetch_macro_context)


@app.get("/api/gdelt-tone")
async def get_gdelt_tone():
    """GDELT DOC 2.0 TimelineTone for Iran oil/military coverage (keyless)."""
    return await _run_sync(data_service.fetch_gdelt_tone)


# ─── Live AIS Vessel Feed (AISStream.io WebSocket) ───────────────────────────
# DEPRECATED: AISStream.io's free tier allows one concurrent WebSocket per key.
# Render's free tier free-restarts the worker often enough that we live in 429
# purgatory (see AIS_DEPRECATION.md / commit history). The endpoints stay so
# old cached frontends don't 404, but they always return empty.

@app.get("/api/vessels/{chokepoint}")
async def get_vessels(chokepoint: str):
    """DEPRECATED. Live AIS replaced by /api/chokepoint-incidents/{chokepoint}.
    Returns an empty list to keep stale frontends quiet."""
    return {"chokepoint": chokepoint, "count": 0, "data": [], "deprecated": True}


@app.get("/api/vessels-status")
async def get_vessels_status():
    """DEPRECATED — see /api/chokepoint-incidents."""
    return {"deprecated": True, "successor": "/api/chokepoint-incidents/{cp}"}


# ─── Chokepoint Incident Overlay (real ACLED + iran-events) ──────────────────

@app.get("/api/chokepoint-incidents/{chokepoint}")
async def get_chokepoint_incidents(chokepoint: str, days: int = 90, limit: int = 200):
    """Real ACLED + Iran-events incidents inside a chokepoint zone.

    Chokepoint must be one of: hormuz, bab, suez. Returns the most recent
    `limit` events from the last `days` days that either fall inside the
    chokepoint's incident bounding box OR are attributed to actors operating
    against shipping there (Houthi for Bab, IRGC for Hormuz). Each record
    carries lat/lon, date, fatalities, source, and notes — all real, all
    geocoded directly from ACLED.
    """
    result = await _run_sync(lambda: data_service.get_chokepoint_incidents(chokepoint, days, limit))
    events = result.get("events", [])
    return {
        "chokepoint": chokepoint,
        "days": days,
        "count": len(events),
        "data": events,
        # Disclosure for the frontend caption: when does the dataset's
        # newest in-zone event sit? If wall-clock cutoff filtered everything
        # out (count==0), the UI uses this to say "ACLED data through X · no
        # events in last N days" instead of going silently empty.
        "zone_newest_date": result.get("zone_newest_date"),
        "wall_clock_cutoff": result.get("wall_clock_cutoff"),
    }


@app.get("/api/chokepoint-incidents-meta")
async def get_chokepoint_incidents_meta():
    """Diagnostic: incident bounding boxes + ACLED fetch meta."""
    return data_service.get_chokepoint_incidents_meta()


@app.get("/api/freshness")
async def get_freshness():
    """Per-source data freshness (newest event date + origin) for the UI.

    Powers the top-right status pill and the chokepoint card "data through"
    captions. Reads memos/caches only — never triggers a live fetch — so it's
    safe to poll on a 60s interval. See `data_service.get_freshness_snapshot`
    for the response schema.
    """
    return data_service.get_freshness_snapshot()


@app.get("/api/live-event-counts")
async def get_live_event_counts(force: int = 0):
    """Live ACLED monthly event counts via the HDX mirror (no auth required).

    Returns guaranteed-fresh aggregate counts (events + fatalities by month)
    for every chokepoint-relevant country: Yemen, Iran, Saudi Arabia, Egypt,
    Lebanon. Refreshed every 6 hours from data.humdata.org which itself
    re-publishes ACLED's weekly drop every Wednesday.

    Use `?force=1` to bypass the 6-hour cache (admin / debugging only).
    """
    return await _run_sync(lambda: data_service.fetch_hdx_event_counts(force=bool(force)))


# ─── Data Refresh ─────────────────────────────────────────────────────────────

_refresh_lock = threading.Lock()
_refresh_in_progress = False

def _do_refresh():
    """Background task: clear caches and re-fetch data in parallel."""
    global _refresh_in_progress
    if not _refresh_lock.acquire(blocking=False):
        return
    _refresh_in_progress = True
    try:
        data_service._acled_token = None
        # Invalidate in-process memos so the refresh actually re-fetches
        data_service._acled_events_memo = None
        data_service._acled_events_memo_ts = 0.0
        data_service._iran_fallback_memo = None
        data_service._thesis_events_cache = None

        # Clear only API-driven caches (preserve master_dataset, thesis_events)
        _api_caches = {"acled_events", "iran_events", "brent_prices", "iran_news",
                       "spr_data", "dxy", "ovx", "china_pmi",
                       "eia_commercial_crude", "eia_cushing_stocks",
                       "fred_wti", "fred_breakeven5", "fred_vix", "fred_hy_yield",
                       "gdelt_tone_90d",
                       # HDX live ACLED mirror — clear so the refresh actually
                       # pulls a new monthly drop (HDX republishes weekly).
                       "hdx_event_counts"}
        for cache_file in config.CACHE_DIR.glob("*.json"):
            if cache_file.stem in _api_caches:
                try:
                    cache_file.unlink()
                except Exception:
                    pass

        # Re-fetch all data sources in parallel — including Tier-1 (EIA / FRED /
        # GDELT) and the no-auth HDX ACLED mirror. Without this, a refresh
        # clears those cache files but never repopulates them, leaving the UI
        # blank until a user-facing request happens to trigger the live fetcher.
        with data_service.ThreadPoolExecutor(max_workers=8) as pool:
            f_events = pool.submit(data_service.fetch_acled_events)
            f_iran = pool.submit(data_service.fetch_iran_events)
            f_brent = pool.submit(data_service.fetch_brent_prices)
            f_news = pool.submit(data_service.fetch_iran_news)
            f_eia = pool.submit(data_service.fetch_eia_inventories)
            f_macro = pool.submit(data_service.fetch_macro_context)
            f_gdelt = pool.submit(data_service.fetch_gdelt_tone)
            f_hdx = pool.submit(data_service.fetch_hdx_event_counts, True)

        events = f_events.result()
        iran = f_iran.result()
        f_brent.result()
        f_news.result()
        # Tier-1 results aren't used here directly — we just need the side
        # effect of re-populated caches.
        for fut in (f_eia, f_macro, f_gdelt, f_hdx):
            try:
                fut.result()
            except Exception as e:
                print(f"[refresh] tier-1 fetcher failed: {e!r}")

        # Update JSON fallbacks if we got real API data
        if len(events) > 1000:
            (config.DATA_DIR / "acled_events.json").write_text(json.dumps(events, default=str))
        if len(iran) > 100:
            (config.DATA_DIR / "iran_events.json").write_text(json.dumps(iran, default=str))
    finally:
        _refresh_in_progress = False
        _refresh_lock.release()


@app.post("/api/refresh")
async def refresh_data(bg: BackgroundTasks):
    """Trigger a background data refresh."""
    if _refresh_in_progress:
        return {"status": "refresh already in progress"}
    bg.add_task(_do_refresh)
    return {"status": "refresh started"}


@app.get("/api/refresh-status")
async def refresh_status():
    """Check if a refresh is currently in progress."""
    return {"in_progress": _refresh_in_progress}


# ─── Startup Preload ─────────────────────────────────────────────────────────

@app.on_event("startup")
async def preload_data():
    """Warm caches in a background thread so the server starts accepting
    requests immediately (critical for Render health-check timing).

    Memory-aware preload: Render's free tier is capped at 512MB. The old
    preload spawned 6 worker threads fetching 11 datasets in parallel at
    boot, which reliably OOM'd the instance. This version:
      - caps concurrency at 2 workers
      - skips ACLED (13MB JSON) entirely; it lazy-loads on first /api/events
        and is then memoized in-process
      - forces gc.collect() between phases to release intermediate DataFrames
      - small per-task timeouts so a stuck fetch never pins memory
    """

    def _preload():
        import concurrent.futures
        import gc
        print("  [preload] Starting background cache warming (low-mem mode)...")
        # Phase 1: small external API caches that master depends on.
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            api_futures = {
                "brent": pool.submit(data_service.fetch_brent_prices),
                "dxy":   pool.submit(data_service.fetch_dxy),
                "ovx":   pool.submit(data_service.fetch_ovx),
            }
            for name, fut in api_futures.items():
                try:
                    fut.result(timeout=20)
                    print(f"  [preload] {name}: OK")
                except Exception as e:
                    print(f"  [preload] {name}: FAILED ({e})")
        gc.collect()

        # Phase 1.5: HDX live ACLED mirror — moved here so the master
        # timeseries extender (Phase 2) sees populated HDX data and can
        # backfill weekly_attacks/daily_attacks for live rows. Without
        # this ordering, master ran first with empty HDX cache, set
        # all live-row weekly_attacks=0, memoized that bad result for
        # 90s, and the §05 scatter showed every live point at x=0.
        try:
            print(f"  [preload] HDX live ACLED mirror: warming...")
            t0 = time.time()
            snap = data_service.fetch_hdx_event_counts()
            countries = list((snap or {}).get("by_country", {}).keys())
            print(f"  [preload] HDX: OK — {len(countries)} countries, newest={snap.get('newest_month')}, took {time.time()-t0:.1f}s")
        except Exception as e:
            print(f"  [preload] HDX: FAILED ({e}) — pill will fall back to bundled ACLED date")
        gc.collect()

        # Phase 2: master + transit counts. These are all small (<1MB each).
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = {
                "master": pool.submit(data_service.load_master_dataset),
                "thesis": pool.submit(data_service.load_thesis_events),
                "iran":   pool.submit(data_service.fetch_iran_events),
                "news":   pool.submit(data_service.fetch_iran_news),
                "suez":   pool.submit(data_service.fetch_suez_transits),
                "mandeb": pool.submit(data_service.fetch_bab_el_mandeb_transits),
                "hormuz": pool.submit(data_service.fetch_hormuz_transits),
            }
            for name, fut in futures.items():
                try:
                    fut.result(timeout=60)
                    print(f"  [preload] {name}: OK")
                except Exception as e:
                    print(f"  [preload] {name}: FAILED ({e})")
        gc.collect()

        # Phase 3: ACLED. Deliberately runs AFTER phases 1+2 have returned
        # so their intermediate DataFrames have been released (gc.collect
        # above). Previously this was skipped because 6-wide parallel
        # preload + ACLED's 13MB blob OOM'd Render free. With the tighter
        # per-phase memory budget above, we can afford to warm ACLED
        # serially on boot so the first visitor doesn't wait 90+ seconds
        # Memory probe helper: logs RSS so an OOM in subsequent phases is
        # diagnosable from Render's log stream rather than a black-box
        # "exceeded memory limit" notification email.
        def _mem_mb():
            try:
                import resource, sys
                rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                # ru_maxrss units differ by platform:
                #   Linux:  kilobytes
                #   macOS:  bytes
                # Detect by sys.platform rather than by magnitude (the
                # magnitude heuristic gave 1.15 TB readings on Linux because
                # 1.13 GB in KB looks like "bytes" by size).
                if sys.platform == "darwin":
                    return round(rss / (1024 * 1024), 1)
                return round(rss / 1024, 1)
            except Exception:
                return None

        # Phase 3: ACLED bulk fetch (heaviest single source, ~70MB Python).
        # HDX already warmed in Phase 1.5 above so master timeseries had
        # data to backfill from.
        # Last in the preload chain so a memory blowup here at most loses
        # the row-level events sidebar — Brent / DXY / OVX / HDX / transits
        # are all already cached and the dashboard is functional without
        # ACLED's row-level data.
        try:
            print(f"  [preload] ACLED: warming cache... (mem={_mem_mb()}MB)")
            t0 = time.time()
            events = data_service.fetch_acled_events()
            print(f"  [preload] ACLED: OK — {len(events)} events in {time.time()-t0:.1f}s, mem={_mem_mb()}MB")
        except Exception as e:
            print(f"  [preload] ACLED: FAILED ({e}) — will lazy-load on first request")
        gc.collect()
        print(f"  [preload] Cache warming complete (mem={_mem_mb()}MB)")

    # Fire-and-forget in a daemon thread — server starts immediately
    t = threading.Thread(target=_preload, daemon=True)
    t.start()

    # ── Periodic cache warmer ──────────────────────────────────────────
    #
    # Eliminates the cold-start 429 pattern: previously, when a cache
    # expired mid-session the next user request paid the cost of a fresh
    # external fetch — and if it 429'd (GDELT especially), the resulting
    # partial cache locked in for the full TTL, leaving the chart silent.
    # By re-fetching just BEFORE each cache TTL elapses, user requests
    # always hit warm cache and rate-limit pressure is amortized.
    #
    # Per-source intervals (chosen well below their cache TTL so refresh
    # runs while the previous data is still valid):
    #   GDELT tone   — 25 min  (partial TTL 30 min)
    #   iran_news    — 60 min  (Google News updates roughly hourly)
    #   Brent        — 6 hours (daily data, 24h TTL)
    #   DXY/OVX      — 6 hours
    #   HDX (ACLED)  — 12 hours
    #   transits     — 12 hours (PortWatch publishes weekly)
    def _periodic_warmer():
        # Stagger the first wakes so they don't pile on the preload thread.
        time.sleep(60)
        intervals = [
            ("gdelt",       25 * 60,  data_service.fetch_gdelt_tone),
            ("iran_news",   60 * 60,  data_service.fetch_iran_news),
            ("brent",        6 * 3600, data_service.fetch_brent_prices),
            ("dxy",          6 * 3600, data_service.fetch_dxy),
            ("ovx",          6 * 3600, data_service.fetch_ovx),
            ("hdx",         12 * 3600, lambda: data_service.fetch_hdx_event_counts(force=True)),
            ("hormuz_tx",   12 * 3600, data_service.fetch_hormuz_transits),
            ("bab_tx",      12 * 3600, data_service.fetch_bab_el_mandeb_transits),
            ("suez_tx",     12 * 3600, data_service.fetch_suez_transits),
            # FMP intraday — keep the hero KPI minute-fresh during market
            # hours. Quote refreshes match the FMP cache TTL (10 min);
            # hourly bars + equity quotes refresh every hour.
            ("fmp_brent",       10 * 60, data_service.fetch_fmp_brent_quote),
            ("fmp_brent_1h",    60 * 60, data_service.fetch_fmp_brent_intraday),
            ("fmp_xom",         60 * 60, lambda: data_service.fetch_fmp_equity_quote("XOM")),
            ("fmp_cvx",         60 * 60, lambda: data_service.fetch_fmp_equity_quote("CVX")),
            ("fmp_oxy",         60 * 60, lambda: data_service.fetch_fmp_equity_quote("OXY")),
        ]
        last = {name: time.time() for name, _, _ in intervals}
        while True:
            try:
                now = time.time()
                for name, every, fn in intervals:
                    if now - last[name] >= every:
                        try:
                            fn()
                            last[name] = now
                        except Exception as e:
                            # Don't crash the warmer on a single fetcher's
                            # exception — log and try again next cycle.
                            print(f"  [warmer] {name}: FAILED ({e})")
                # Sleep 60s between cycles. Each iteration only wakes the
                # fetchers whose interval has elapsed, so the loop is cheap.
                time.sleep(60)
            except Exception as e:
                # Cosmic-ray defense — if anything in the warmer loop ever
                # explodes, log and keep going. The dashboard tolerates a
                # missed refresh; it doesn't tolerate a dead warmer thread.
                print(f"  [warmer] loop error: {e}")
                time.sleep(60)

    threading.Thread(target=_periodic_warmer, daemon=True).start()


# ─── Frontend Entry Point ────────────────────────────────────────────────────


def _asset_cache_buster() -> str:
    """Compute a cache-buster string from the latest asset mtime.

    Replaces the manual `?v=YYYYMMDDx` bump that had to be edited on every
    asset change. The buster updates automatically the moment any asset
    file is saved, so a redeploy guarantees clients fetch the new bundle.
    Cached for the process lifetime — the value is stable across requests
    until the server restarts (which happens on every deploy).
    """
    import hashlib
    asset_dir = config.BASE_DIR / "assets"
    mtimes = []
    for p in sorted(asset_dir.glob("*.js")):
        try:
            mtimes.append(p.stat().st_mtime)
        except OSError:
            pass
    for p in sorted(asset_dir.glob("*.css")):
        try:
            mtimes.append(p.stat().st_mtime)
        except OSError:
            pass
    try:
        mtimes.append((config.BASE_DIR / "dashboard.html").stat().st_mtime)
    except OSError:
        pass
    if not mtimes:
        return "0"
    digest = hashlib.md5(",".join(f"{t:.0f}" for t in mtimes).encode()).hexdigest()[:10]
    return digest


_ASSET_CACHE_BUSTER = _asset_cache_buster()


@app.get("/")
def serve_dashboard():
    """Serve dashboard.html with the manual `?v=YYYYMMDD<x>` query strings
    rewritten to an mtime-hash buster computed at startup. Edit any asset,
    redeploy, and clients fetch the new bundle automatically — no more
    hand-bumping eight `<script src=...?v=...>` lines on every change.
    """
    html_path = config.BASE_DIR / "dashboard.html"
    try:
        html = html_path.read_text(encoding="utf-8")
        # Replace any existing ?v=... with the live buster. Pattern matches
        # both v=YYYYMMDD and v=YYYYMMDDx forms.
        import re
        html = re.sub(r"\?v=[A-Za-z0-9]+", f"?v={_ASSET_CACHE_BUSTER}", html)
        from fastapi.responses import HTMLResponse as _HTMLResponse
        response = _HTMLResponse(content=html)
    except OSError:
        # Fallback: serve raw file if read fails
        response = FileResponse(html_path)
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response


# ─── Run ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\n  Red Sea Crisis Intelligence Dashboard")
    print(f"  ─────────────────────────────────────")
    print(f"  Open in browser: http://localhost:{config.PORT}")
    print(f"  Press Ctrl+C to stop\n")
    try:
        uvicorn.run(
            "app:app",
            host=config.HOST,
            port=config.PORT,
            reload=True,
            reload_dirs=[str(config.BASE_DIR)],
        )
    except (PermissionError, OSError):
        # Fallback: reload not supported in this environment (e.g. /tmp on macOS)
        uvicorn.run(app, host=config.HOST, port=config.PORT)
