"""
Red Sea Crisis Intelligence Dashboard - FastAPI Backend
Run: python app.py
"""
import asyncio
import json
import threading
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

    return {
        "server_time_utc": datetime.now(timezone.utc).isoformat(),
        "acled": {
            "credentials_configured": acled_meta["credentials_configured"],
            "last_fetch_source": acled_meta["source"],
            "last_fetch_utc": _iso(acled_meta["ts"]),
            "last_fetch_error": acled_meta["error"],
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


@app.get("/api/events")
async def get_events():
    """ACLED event data for map and table."""
    events = await _run_sync(data_service.fetch_acled_events)
    return {"count": len(events), "data": events}


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
    Serves cached/fallback data immediately."""
    # Try cache first, then JSON fallback
    iran_events = data_service._read_cache("iran_events", 3600)
    if not iran_events:
        iran_events = data_service._load_iran_json_fallback()
    if not iran_events:
        iran_events = await _run_sync(data_service.fetch_iran_events)

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
                       "spr_data", "dxy", "ovx", "china_pmi"}
        for cache_file in config.CACHE_DIR.glob("*.json"):
            if cache_file.stem in _api_caches:
                try:
                    cache_file.unlink()
                except Exception:
                    pass

        # Re-fetch all data sources in parallel
        with data_service.ThreadPoolExecutor(max_workers=4) as pool:
            f_events = pool.submit(data_service.fetch_acled_events)
            f_iran = pool.submit(data_service.fetch_iran_events)
            f_brent = pool.submit(data_service.fetch_brent_prices)
            f_news = pool.submit(data_service.fetch_iran_news)

        events = f_events.result()
        iran = f_iran.result()
        f_brent.result()
        f_news.result()

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
        # NB: ACLED is intentionally NOT preloaded — it's 13MB and its on-disk
        # cache is already warm from prior deploys. First /api/events call
        # populates the in-process memo; subsequent calls are instant.
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
        print("  [preload] Cache warming complete (ACLED deferred to first request)")

    # Fire-and-forget in a daemon thread — server starts immediately
    t = threading.Thread(target=_preload, daemon=True)
    t.start()


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
