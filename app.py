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

import config
import data_service


def _run_sync(fn, *args):
    """Run a blocking function in the default executor."""
    loop = asyncio.get_running_loop()
    return loop.run_in_executor(None, fn if not args else partial(fn, *args))

app = FastAPI(title="Red Sea Crisis Intelligence Dashboard")

# Serve static files (HTML, CSS, JS) with no-cache headers

class NoCacheStaticMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/static"):
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

app.add_middleware(NoCacheStaticMiddleware)
app.mount("/static", StaticFiles(directory=config.BASE_DIR / "static"), name="static")


# ─── API Endpoints ───────────────────────────────────────────────────────────

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


@app.get("/api/iran-events")
async def get_iran_events():
    """Iran-related ACLED events + curated major events + live news."""
    # Fetch ACLED events and news in parallel (curated is in-memory, instant)
    acled_task = _run_sync(data_service.fetch_iran_events)
    news_task = _run_sync(data_service.fetch_iran_news)
    acled_events, news = await asyncio.gather(acled_task, news_task)
    curated = data_service.get_merged_curated_events()
    result = {"count": len(acled_events), "data": acled_events, "curated": curated, "news": news}
    err = data_service.get_iran_fetch_error()
    if err:
        result["error"] = err
    return result


@app.get("/api/iran-impact")
async def get_iran_impact():
    """Oil price impact analysis around Iran events."""
    # Fetch iran events and brent prices in parallel
    iran_task = _run_sync(data_service.fetch_iran_events)
    brent_task = _run_sync(data_service.fetch_brent_prices)
    iran_events, brent_prices = await asyncio.gather(iran_task, brent_task)
    result = data_service.compute_iran_impact(iran_events, brent_prices)
    result["brent_prices"] = brent_prices
    return result


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


# ─── Frontend Entry Point ────────────────────────────────────────────────────

@app.get("/")
def serve_dashboard():
    response = FileResponse(config.BASE_DIR / "static" / "index.html")
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
