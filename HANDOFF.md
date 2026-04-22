# Chokepoint Intel — Developer Handoff

Live Red Sea / Strait of Hormuz energy-security dashboard. Single-page HTML front-end + FastAPI backend that aggregates ACLED, EIA, FRED, yfinance, and IMF PortWatch data.

---

## 1. What's in this folder

```
design_handoff_chokepoint_intel/
├── dashboard.html              # Single-page front-end (entry point)
├── assets/
│   ├── api.js                  # Front-end API client (wraps /api/*)
│   ├── hydrate.js              # Fetches data, populates DOM + globals
│   ├── data.js                 # Geographic primitives (ports, arcs) — physical constants only
│   ├── charts.js               # Chart.js rendering (awaits CP.hydrated)
│   ├── globe.js                # D3 orthographic globe
│   ├── tacmap.js               # D3 tactical map (Red Sea / Gulf)
│   ├── app.js                  # Tab nav, timeline scrubber, data-explorer overlay
│   └── app.css
├── app.py                      # FastAPI entry — routes, startup preload, refresh
├── config.py                   # Env-var loading + paths + TTLs
├── data_service.py             # All external API adapters (ACLED/EIA/FRED/yfinance/IMF)
├── refresh_data.py             # CLI to manually warm the cache
├── requirements.txt
├── render.yaml                 # Render.com blueprint (web service, health check)
└── HANDOFF.md                  # This file
```

At runtime the backend writes two directories next to `app.py`:
- `.cache/` — JSON caches keyed by source (TTLs in `config.py`)
- `data/` — fallback CSVs/JSON so the UI never renders blank when an upstream is down

---

## 2. Local dev — 60-second bootstrap

```bash
cd design_handoff_chokepoint_intel
python -m venv .venv && source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env     # create this — see "API keys" below
python app.py            # opens on http://localhost:8000
```

The first load takes ~20 seconds to warm all caches (see `preload_data()` in `app.py`). Subsequent loads are instant. The header status pill turns **green / LIVE** once `/api/health` responds; it stays **red / OFFLINE** if the backend is unreachable.

### API keys (`.env`)

Create a `.env` in this folder. None are technically required — every endpoint falls back to cached JSON in `data/` — but real-time behavior needs them:

```
EIA_API_KEY=...             # https://www.eia.gov/opendata/register.php  (free)
FRED_API_KEY=...            # https://fredaccount.stlouisfed.org/apikeys (free)
ACLED_USERNAME=email@...    # https://developer.acleddata.com/ (free academic tier)
ACLED_PASSWORD=...
```

Without an ACLED key the map shows cached events (up to today's cached snapshot). Without an EIA key the Brent price series stops updating but the last known value is shown.

---

## 3. Deploy — Render.com (one-click)

`render.yaml` is a blueprint. In the Render dashboard: **New → Blueprint → connect this repo** and Render will read it. Fill in the four secrets (EIA/FRED/ACLED) in the Render env-var UI — they are marked `sync: false` so they don't get committed.

Health check is `/api/health` (returns immediately, does not trigger data loading). Startup preload runs in a daemon thread so Render's health probe doesn't time out.

To deploy elsewhere: any Python 3.11 host that can run `uvicorn app:app --host 0.0.0.0 --port $PORT` will work. The app is stateless; `.cache/` and `data/` are regenerated on first hit.

---

## 4. How live updates work

The front-end has **no hardcoded analytical values** (prices, threats, event counts, vessel counts). Anything that can change is fetched from the backend.

Data flow on page load:

1. `dashboard.html` loads → `api.js` → `hydrate.js` fires on DOMContentLoaded.
2. `hydrate.js` calls `/api/health` (flips the status pill) and in parallel:
   - `/api/master` — KPIs (latest Brent, OVX, DXY, correlations)
   - `/api/brent` — price series → sparkline + 30D range
   - `/api/events` — ACLED events → attack hotspots, incident counts
   - `/api/iran-events` — curated events + news feed
   - `/api/suez-transits`, `/api/bab-el-mandeb-transits`, `/api/hormuz-transits` — chokepoint throughput
3. Populates `window.CHOKEPOINTS`, `window.ATTACKS`, `window.BRENT_SPARK`, `window.FEED`, `window.IRAN_EVENTS`, and fills every DOM node with a live value.
4. Resolves `window.CP.hydrated` → `charts.js` renders all charts against the now-populated state.
5. Dispatches `window` event `data-hydrated` with the full state payload.
6. Auto-refreshes every **5 minutes** (`setInterval` in `hydrate.js`) — invalidates the in-memory cache and re-runs the whole pipeline.

Manual refresh from the console: `CP.refresh()`.

Server-side refresh: `POST /api/refresh` triggers `_do_refresh()` in a background thread — clears disk cache for API-driven sources and re-fetches ACLED, Iran events, Brent, and news in parallel. `GET /api/refresh-status` polls progress.

### Missing-value policy

If a field cannot be resolved, the DOM shows em-dash (`—`), never a fabricated number. If ≥5 of the 7 parallel fetches fail, the status pill flips to "PARTIAL · some data stale".

---

## 5. Backend endpoints (contract)

All endpoints return JSON. Listed in `app.py`.

| Method | Path | Description | Cache TTL |
|---|---|---|---|
| GET | `/api/health` | Fast liveness probe (no data load) | — |
| GET | `/api/master` | KPIs + timeseries + correlation matrix | in-proc |
| GET | `/api/events` | ACLED events (Red Sea / Yemen / Iran theater) | 24h |
| GET | `/api/thesis-events` | 726 ACLED-verified maritime events | in-proc |
| GET | `/api/brent` | Daily Brent crude (EIA) | 24h |
| GET | `/api/dxy` | US Dollar Index (yfinance) | 24h |
| GET | `/api/ovx` | Oil Volatility Index (yfinance) | 24h |
| GET | `/api/spr` | Strategic Petroleum Reserve (EIA) | 24h |
| GET | `/api/china-pmi` | China PMI (FRED) | 7d |
| GET | `/api/suez-transits` | Monthly Suez Canal volumes (IMF PortWatch) | 24h |
| GET | `/api/bab-el-mandeb-transits` | Monthly Bab el-Mandeb volumes | 24h |
| GET | `/api/hormuz-transits` | Monthly Hormuz volumes | 24h |
| GET | `/api/iran-events` | Iran ACLED + curated + news (composite) | 1h / 2h |
| GET | `/api/iran-impact` | Brent price impact around Iran events | 1h |
| GET | `/api/hypothesis` | Econometric hypothesis test results | 10m |
| GET | `/api/comparative` | Houthi vs Iran comparative dataset | 5m |
| GET | `/api/iran-intensity` | Iran conflict intensity time series | 5m |
| GET | `/api/hormuz-status` | Hormuz disruption tracker | 5m |
| GET | `/api/war-phases` | Phase annotations for timeline | 1h |
| GET | `/api/iran-regression` | OLS regression on Iran war data | 10m |
| POST | `/api/refresh` | Triggers background re-fetch (all live sources) | — |
| GET | `/api/refresh-status` | `{in_progress: bool}` | — |

Shape of list endpoints: `{ count: int, data: [...] }`. Master/hypothesis/regression return their payload directly.

---

## 6. Extending

### Add a new data source
1. Add a fetcher in `data_service.py` following the `fetch_*` pattern (uses `_read_cache`/`_write_cache` + `concurrent.futures`).
2. Expose it in `app.py` with a `@app.get("/api/your-source")` that calls `_run_sync(...)`.
3. Add a method to `api.js` → `window.API.yourSource()`.
4. In `hydrate.js` call it inside the `Promise.all` block in `loadOnce()` and populate a DOM element or a `window.*` global.
5. Add it to the `preload_data()` startup ThreadPoolExecutor so it's warm on boot.

### Add a chart
1. Get a `<canvas id="yourChart">` into `dashboard.html`.
2. In `charts.js` add a `renderYourChart(state)` and call it from the main `async` IIFE after `await window.CP.hydrated`.
3. Pull data from `state` (the resolved value of `CP.hydrated`) or `window.*` globals populated by the hydrator.

### Change refresh cadence
- Front-end: `setInterval(..., 5 * 60 * 1000)` at the bottom of `hydrate.js`.
- Back-end cache TTLs: `CACHE_TTL_*` in `config.py`.
- Server-side scheduled refresh: wire a cron or Render cron job to `POST /api/refresh`.

---

## 7. Known gaps / next steps for the dev team

- **Map attack layer.** `tacmap.js` and `globe.js` were scaffolded with a demo attack array. `hydrate.js` already builds `window.ATTACKS` from live ACLED data — connect `tacmap.js` to it by replacing its internal `attacks` const with `window.ATTACKS` and adding a `data-hydrated` event listener that re-runs its marker layer. Same pattern for `globe.js`.
- **Timeline event bus.** `app.js` emits a `timeline-set` CustomEvent when the scrubber moves (with `{pct, date, ts}`). Nothing listens yet — wire charts / map / feed to filter against `ts` so scrubbing back to e.g. Dec 2023 shows that state.
- **SSE / WebSocket push.** Currently the client polls on a 5 min interval. If you want true push (e.g. new ACLED events as they land), add a `/api/stream` SSE endpoint that emits when `_do_refresh()` writes new data, and swap the interval in `hydrate.js` for an `EventSource`.
- **Tests.** Smoke test for each `/api/*` endpoint (status + schema) would be a good first CI check — none exist yet.
- **Error telemetry.** `hydrate.js` currently `console.warn`s partial failures. Pipe to Sentry / Logflare if desired.

---

## 8. Troubleshooting

**Page loads but everything shows `—`** → backend not reachable or all API keys missing. Open devtools → Network → filter `/api/`. Check `/api/health`. If 200 OK, check individual endpoints — a 500 usually means an upstream API rejected the key.

**Status pill stays red** → `/api/health` failed. Make sure `python app.py` is running and the front-end is loading from the same origin (not `file://`). Open `http://localhost:8000/` not the raw `dashboard.html`.

**"OFFLINE · using cache"** → Some endpoints failed. Data shown is the last-known cached value from `.cache/`. If `.cache/` is missing, the seeded JSON in `data/` is used. Run `python refresh_data.py` to manually re-warm.

**Slow first render on Render.com** → Expected. First request after a cold start triggers the preload thread (~20s). The health check endpoint responds instantly so Render doesn't kill the pod.

**ACLED 401/403** → Credentials wrong, or you haven't accepted the ACLED T&C on their developer portal.

---

_Authored by the design team. Ship it._
