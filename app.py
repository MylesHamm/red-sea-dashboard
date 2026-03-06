"""
Red Sea Crisis Intelligence Dashboard - FastAPI Backend
Run: python app.py
"""
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

import config
import data_service

app = FastAPI(title="Red Sea Crisis Intelligence Dashboard")

# Serve static files (HTML, CSS, JS)
app.mount("/static", StaticFiles(directory=config.BASE_DIR / "static"), name="static")


# ─── API Endpoints ───────────────────────────────────────────────────────────

@app.get("/api/master")
def get_master_data():
    """Full master dataset: timeseries, KPIs, price windows, correlation."""
    return data_service.load_master_dataset()


@app.get("/api/events")
def get_events():
    """ACLED event data for map and table."""
    events = data_service.fetch_acled_events()
    return {"count": len(events), "data": events}


@app.get("/api/brent")
def get_brent():
    """Live Brent crude prices from EIA API."""
    prices = data_service.fetch_brent_prices()
    return {"count": len(prices), "data": prices}


@app.get("/api/dxy")
def get_dxy():
    """US Dollar Index from yfinance."""
    data = data_service.fetch_dxy()
    return {"count": len(data), "data": data}


@app.get("/api/ovx")
def get_ovx():
    """Oil Volatility Index from yfinance."""
    data = data_service.fetch_ovx()
    return {"count": len(data), "data": data}


@app.get("/api/spr")
def get_spr():
    """Strategic Petroleum Reserve data from EIA."""
    data = data_service.fetch_spr_data()
    return {"count": len(data), "data": data}


@app.get("/api/china-pmi")
def get_china_pmi():
    """China business confidence from FRED."""
    data = data_service.fetch_china_pmi()
    return {"count": len(data), "data": data}


@app.get("/api/hypothesis")
def get_hypothesis():
    """Hypothesis test results from econometric analysis."""
    return data_service.get_hypothesis_results()


@app.get("/api/iran-events")
def get_iran_events():
    """Iran-related ACLED events + curated major events."""
    acled_events = data_service.fetch_iran_events()
    curated = data_service.get_curated_iran_events()
    return {"count": len(acled_events), "data": acled_events, "curated": curated}


@app.get("/api/iran-impact")
def get_iran_impact():
    """Oil price impact analysis around Iran events."""
    iran_events = data_service.fetch_iran_events()
    brent_prices = data_service.fetch_brent_prices()
    return data_service.compute_iran_impact(iran_events, brent_prices)


# ─── Frontend Entry Point ────────────────────────────────────────────────────

@app.get("/")
def serve_dashboard():
    return FileResponse(config.BASE_DIR / "static" / "index.html")


# ─── Run ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\n  Red Sea Crisis Intelligence Dashboard")
    print(f"  ─────────────────────────────────────")
    print(f"  Open in browser: http://localhost:{config.PORT}")
    print(f"  Press Ctrl+C to stop\n")
    uvicorn.run(app, host=config.HOST, port=config.PORT)
