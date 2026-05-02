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
import ais_service


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
    events = await _run_sync(lambda: data_service.get_chokepoint_incidents(chokepoint, days, limit))
    return {
        "chokepoint": chokepoint,
        "days": days,
        "count": len(events),
        "data": events,
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

        # Phase 3: HDX live ACLED mirror — runs BEFORE the heavy ACLED bulk
        # so even if ACLED OOM-restarts the worker, the freshness pill and
        # the §01 chart's post-thesis bars come up on the next boot.
        # Streaming openpyxl reader keeps peak memory at ~5MB total for
        # all 5 country workbooks (was ~150MB with pandas read_excel).
        try:
            print(f"  [preload] HDX live ACLED mirror: warming... (mem={_mem_mb()}MB)")
            t0 = time.time()
            snap = data_service.fetch_hdx_event_counts()
            countries = list((snap or {}).get("by_country", {}).keys())
            print(f"  [preload] HDX: OK — {len(countries)} countries, newest={snap.get('newest_month')}, took {time.time()-t0:.1f}s, mem={_mem_mb()}MB")
        except Exception as e:
            print(f"  [preload] HDX: FAILED ({e}) — pill will fall back to bundled ACLED date")
        gc.collect()

        # Phase 4: ACLED bulk fetch (heaviest single source, ~70MB Python).
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

    # Live AIS feed: DISABLED. AISStream.io free tier (one concurrent
    # WebSocket per key) is incompatible with Render free-tier deploy
    # cycles — the new worker's connection always loses to the old worker's
    # not-yet-timed-out socket, producing a permanent HTTP 429 retry loop.
    # The `ais_service` module is preserved so reactivation is one line if
    # we ever migrate to a paid AIS provider with HTTP polling. The chokepoint
    # maps now show live ACLED incidents from /api/chokepoint-incidents/{cp}.
    # ais_service.start()


# ─── Frontend Entry Point ────────────────────────────────────────────────────

@app.get("/")
def serve_dashboard():
    response = FileResponse(config.BASE_DIR / "dashboard.html")
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
