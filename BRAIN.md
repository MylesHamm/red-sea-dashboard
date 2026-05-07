# Chokepoint Intel Dashboard — Session Brain

**Last consolidated:** 2026-05-07
**Owner:** Myles Hamm (mhamm@mercyhurst.edu) — Mercyhurst MS Applied Intelligence thesis
**Deployed:** Hugging Face Spaces — `mhamm18/red-sea-dashboard`
**Repo root:** `/Users/myleshamm/Desktop/Intel/Thesis/Dashboard/`

This file is the single source of context for picking up work on this dashboard without re-reading the full transcript. Update it when state changes.

---

## 1. What this dashboard is

Live FastAPI + Chart.js single-page dashboard tracking the **US-Iran war scenario** (war onset `2026-02-28`, today `2026-05-07`) and its impact on **Brent oil prices** via maritime chokepoints (Hormuz, Bab el-Mandeb, Suez). Built for the thesis committee — every panel must reflect real, live, oil-impactful events.

**Hard rule:** No hardcoded analytical values. Every number on the page traces back to an external API or a config constant.

---

## 2. Architecture (one-paragraph mental model)

`config.py` is the single source of truth (war date, chokepoint refs, threat tiers, FMP key). Backend `data_service.py` wraps all upstream APIs with disk-cache TTLs and a periodic warmer thread. `app.py` exposes `/api/*` plus a canonical `/api/dashboard-state` (composed KPI bundle) and `/api/constants` (config exposed to JS as `window.CONSTANTS`). Frontend hydration is server-composed first; `assets/hydrate.js` fills DOM, `assets/charts.js` renders Chart.js, `assets/app.js` runs the state poller. Cache busting is automatic via mtime hash of `assets/` injected into served HTML.

---

## 3. Data sources & their quirks

| Source | Used for | TTL | Quirks |
|---|---|---|---|
| **ACLED** (OAuth) | Houthi/maritime events | 6h | 12-month embargo on free tier → HDX humanitarian mirror fills the gap |
| **HDX** | ACLED backfill | 12h | bundled JSON fallback in `data/` |
| **FRED** | DCOILBRENTEU, DTWEXBGS, OVXCLS, VIXCLS, BAMLH0A0HYM2EY | 1h (DXY/OVX) | DTWEXBGS publication lag = 3-5 business days; rescale factor ≈0.8145 to map broad-index → DXY scale |
| **FMP** (`/stable/quote`) | Intraday Brent (`BZUSD`), equity quotes | 10min | Free-tier 429-prone → 30-min negative-cache backoff per endpoint in `_FMP_RATE_LIMIT_BACKOFF` |
| **GDELT DOC 2.0** | TimelineTone + TimelineVolRaw | 25min | Cache flagged `_partial: True` when partial |
| **IMF PortWatch** | Port traffic (ArcGIS feature server) | — | — |
| **Google News RSS** | Curated war/iran news | 60min | HF Spaces shared IP intermittently blocked → preserve prior cache on empty fetch, browser-grade headers, timeout 12s, concurrency 3 |
| **yfinance** | DXY primary attempt (DTWEXBGS fallback) | 1h | — |

---

## 4. Key files (what each one owns)

- **`config.py`** — `WAR_ONSET_DATE`, `CHOKEPOINT_REF_FLOW_MBD`, `INCIDENT_BOUNDING_BOXES`, `INCIDENT_KEYWORDS`, `THREAT_TIERS`, `FMP_API_KEY`, all `CACHE_TTL_*`. **Touch this, not the consumers.**
- **`data_service.py`** — all upstream adapters. ~237KB. Hot zones:
  - `_do_fetch_acled_events` — runs `_is_maritime_relevant` filter at the **dataset level** before cache write (logs `ACLED dataset-level filter: kept N/M …`).
  - `_load_acled_fallback` — same filter at runtime for bundled JSON.
  - `_MARITIME_TARGETS` / `_MARITIME_CONTEXT` / `_MARITIME_PORTS` — structured tuples; matches notes/sub_event_type/location/admin1 only (NOT actor1/actor2 — actor names like "houthi" aren't maritime indicators).
  - `_compute_dxy_rescale_factor()` ≈ 0.8145 (broad index → DXY scale).
  - `_fmp_get` with `_FMP_RATE_LIMIT_BACKOFF` (30-min negative cache on 429).
  - FRED 5xx retry with backoff.
  - `iran_news` scraper: 17 queries, per-date cap of 20 articles after sort.
- **`app.py`** — routes, `/api/dashboard-state` (canonical KPI composer with `weekly_oil_events`, `energy_equities`, FMP-anchored Brent), `/api/constants`, periodic warmer thread, mtime-hash cache buster.
- **`dashboard.html`** — single-page UI. Tac-map filter chips: `"Vessel attacks only"` (default `data-tac-mode="tanker"`) + `"All maritime events"` (`data-tac-mode="maritime"`). §02.1 subtitle discloses dataset-level filtering. Hero foot data hooks: `[data-30d-range]`, `[data-since-war]`, `[data-war-prem]`. Methodology footer in `<footer class="thesis-footer">`.
- **`assets/hydrate.js`** — `hydrateTacStats` reuses tac-map's filter via `window.__isMaritimeRelevant` / `window.__isTankerSpecific`. `hydrateHero` defers to FMP via `fmpApplied` check. `classifyThreat` reads `window.CONSTANTS.threat_tiers`.
- **`assets/charts.js`** — `renderDxyOvx` writes `[data-dxy-asof]` / `[data-ovx-asof]`. `renderPriceVsAttacks` reads `window.__dashState.weekly_oil_events` for post-Oct 2025 bars. §09 timeline buckets events by type+source (not per-event) with `interaction.mode: 'point'`.
- **`assets/tacmap.js`** — `__tacFilterMode` defaults `'tanker'`. `isMaritimeRelevant(e)` / `isTankerSpecific(e)` read server flags `e.maritime` / `e.tanker_target` first, fallback keyword matching.
- **`assets/app.js`** — `_applyHeroBrentFromState` is the **canonical hero writer** (single-writer rule). `refreshDashboardState` poller. `refreshHeroIncidentsKpi` does deduped union of curated + iran_news, per-day cap of 10.
- **`tests/test_smoke.py`** — 82 Playwright assertions. Cross-panel consistency tests: `test_hero_kpi_dominates_chokepoint_cards`, `test_weekly_events_consistent_with_hero_kpi`, `test_priceattack_chart_bars_match_card_scope`.

---

## 5. Standing rules (don't break these)

1. **No hardcoded analytical values.** Constants live in `config.py`. Live data lives upstream.
2. **Single-writer rule for DOM.** A given element has exactly one canonical writer; everyone else delegates. Hero Brent goes through `_applyHeroBrentFromState`.
3. **Cross-panel consistency.** Same metric, same definition, everywhere. Asserted in smoke tests:
   - `hero ≥ max(chokepoint card)`
   - `weekly-last-4 ÷ hero-30d ∈ [0.5, 2.0]`
4. **TTL ≤ warmer interval.** Otherwise the warmer hits cache and skips wire (this caused 5-day-stale DXY).
5. **Filter at the dataset, not the dashboard.** Maritime relevance is applied in `_do_fetch_acled_events` before cache write. Inland Yemen civil-war events never reach the frontend.
6. **Disclose freshness.** Stale data gets an "AS OF" caption (`[data-dxy-asof]`, etc.), never a fake recent label.

---

## 6. Methodology constants worth knowing

- **GJR-GARCH(1,1,1)** regression: Oct 2023 – Sep 2025, N=505. This is the volatility model cited in the thesis footer.
- **DXY rescale factor:** ≈ 0.8145 (DTWEXBGS broad index → DXY).
- **Threat tiers:** Defined in `config.THREAT_TIERS`, exposed via `/api/constants`.

---

## 7. Recently fixed bugs (don't reintroduce)

- §07 DXY 95→120 jump → DTWEXBGS was being labeled DXY. Fixed with yfinance-first + rescale fallback.
- "13d ago" labels on 13-month-old data → wall-clock cutoff in `get_chokepoint_incidents` + caption disclosure.
- Backend `source_type: "news_auto"` (not `auto: True`) — `hydrateIranEvents` reads the right field.
- §09 tooltip explosion — events bucketed by type+source, not per-event.
- Hero KPI multi-writer race — `_applyHeroBrentFromState` is the single writer.
- Hero count too low — news scraper 3→17 queries, broadened `actor_terms`, per-date cap of 20 after sort (not before).
- Cards showing identical events — keywords were cross-matching across chokepoints; tightened back to chokepoint-specific in `INCIDENT_KEYWORDS`.
- Hero=11 on deployed — Google News blocked on HF shared IP; preserve prior cache on empty fetch, browser-grade headers, timeout 12s, concurrency 3.
- FMP 429 spam — frontend's 60s polling; added 30-min negative cache.
- DXY 5 days stale — TTL 24h vs warmer 6h; set both to 1h.
- ACLED filter at the wrong layer — moved from UI chips into `_do_fetch_acled_events`.

---

## 8. Most recent commit

`4e49003` — dataset-level maritime filter for ACLED. Tightened `_MARITIME_KEYWORDS` into structured tuples, dropped "houthi"/"ansar allah" from indicator list (those are actor names), `_is_maritime_relevant` matches notes+sub_event_type+location+admin1 only, applied at end of `_do_fetch_acled_events` before cache write and in `_load_acled_fallback`. Frontend simplified to two filter chips. §02.1 subtitle updated. 82/82 smoke tests passed. Pushed to HF Spaces.

---

## 9. Outstanding loose ends

- **Verify on deployed.** Confirm HF Spaces logs show the `ACLED dataset-level filter: kept N/M maritime-relevant events` line on first fetch after the latest deploy.

**Resolved:**
- FMP_API_KEY is set as an HF Space secret named `FMP_API_KEY` (confirmed 2026-05-07). Intraday Brent (`BZUSD`) and energy equity quotes are live on the deployed dashboard.

---

## 10. Sandbox / dev environment quirks

- `h11` is sandbox-blocked → uvicorn must run with `httptools` backend.
- Dev server: `dev_server.py`.
- Playwright smoke suite: `pytest tests/test_smoke.py`.
- GitHub Actions CI runs the smoke suite on push.
- Auto cache-buster (mtime hash of `assets/`) means hard-refresh isn't needed in dev.

---

## 11. How to pick up next session

1. Read this file.
2. `git log --oneline -10` to see what landed since this brain was last consolidated.
3. If the user asks about a specific panel, grep `dashboard.html` for the section ID and trace: HTML hook → `hydrate.js` writer → backend route → `data_service.py` fetcher.
4. Don't add UI band-aids when the fix belongs in the data layer. Filter at the dataset.
5. Update this file when architectural decisions change.
