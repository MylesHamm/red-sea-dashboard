# Chokepoint Intel Dashboard — Session Brain

**Last consolidated:** 2026-06-12
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

## 8b. Intelligence-product features (added 2026-06-12, commit 5452121)

- **§00 Daily SITREP** (`#sitrepCard`, `_updateSitrep` in app.js): server composes `sitrep` in dashboard-state — top 3 developments scored by `_score_news_headline` (same scorer as §09 promotion), price action, event delta, vol regime.
- **LIVE VOL REGIME** (`data-kpi="volRegime"`): server computes rolling 5d/21d realized vol from Brent log returns; 21d ranked as percentile vs thesis-window rolling-21d distribution; tiers in `config.VOL_REGIME_TIERS` (CALM <p50, ELEVATED ≥p50, EXTREME ≥p85). The thesis↔live bridge.
- **TANKERS vs MAJORS** (`data-kpi="freightSpread"`): `fetch_energy_equity_tape()` — FMP for XOM/CVX (free tier 402s on OXY/FRO/STNG/TNK + on batch queries!), `_yahoo_quote_direct()` v8-chart fill for the rest. Spread = tanker basket %chg − majors basket %chg = freight/chokepoint-risk proxy.
- **Alert journal**: dashboard-state composer appends fired alerts (day+direction+band dedup) to `alert_log` cache; `/api/alert-log`; count/last in SITREP foot. Resets on Space rebuild (ephemeral FS) — UI says "since <log start>".
- **System-health grid** (`#systemHealthGrid`, `renderSystemHealth` in hydrate.js): generic render of every /api/freshness source — date, origin badge, age-colored dot (monthly feeds get 45d green window).
- **Keep-alive**: `.github/workflows/keep-alive.yml` pings the Space every 30 min from GitHub Actions cron — prevents HF free-tier 48h sleep, keeps warmer running 24/7, doubles as outage alarm.
- **FMP free-tier gotchas (learned the hard way)**: batch quotes are premium (402); symbol universe excludes OXY/FRO/STNG/TNK (402 "not available under your current subscription"); `_fmp_get` now negative-caches 402s for 24h. The yfinance LIBRARY breaks intermittently (Yahoo blocks its session bootstrap) — direct `query1.finance.yahoo.com/v8/finance/chart/<sym>` requests with plain Mozilla UA keep working; `_yahoo_quote_direct()` is the reusable helper.

## 9. Live-update architecture (added 2026-06-12)

- **SSE push**: `/api/stream` (app.py) pushes the full composed dashboard-state whenever the warmer's `_bump_state_version()` fires (any fetcher ran), plus `ping` heartbeats every 25s. `SelectiveGZipMiddleware` exempts the stream from gzip (gzip buffering kills SSE). Frontend `EventSource` in app.js consumes it; the 60s poller is now a **watchdog** that only fires when the stream has been silent >90s.
- **Visibility-aware**: watchdog skips when `document.hidden`; `visibilitychange` → instant invalidate+refresh on return.
- **Tick flash**: `_tickFlash()` in app.js — hero Brent + threat-strip flash green/red (`tickUp`/`tickDown` CSS) when price changes between applies. Single-writer preserved (only `_applyHeroBrentFromState` triggers).
- **Brent quote chain**: `fetch_live_brent_quote()` (data_service) = FMP real-time → yfinance `BZ=F` (~15-min delayed, no key/quota) → EIA daily settle. `source` field flows to the hero badge: `INTRADAY · FMP` / `INTRADAY · YF · ~15MIN DELAY` / `EIA DAILY`. Never label delayed as real-time.
- **Market alert strip**: `_updateMarketAlert()` (app.js) renders above `.kpi-deck` when |24h Brent %| ≥ `config.BRENT_ALERT_THRESHOLD_PCT` (2.0, exposed via /api/constants). Dismissal is per-signature (`direction:int-band`) in sessionStorage.

## 10. Outstanding loose ends

- (none currently — HDX recovered upstream 2026-06; corrupt weekly publish was replaced. CI green with upstream-aware skips.)

**Resolved:**
- FMP_API_KEY is set as an HF Space secret named `FMP_API_KEY` (confirmed 2026-05-07). Intraday Brent (`BZUSD`) and energy equity quotes are live on the deployed dashboard.
- HDX yemen "Bad magic number" (May 2026) was a genuinely corrupt upstream weekly file; HDX replaced it. Retry + stale-fallback in `_do_fetch_hdx_event_counts` covered the gap.

---

## 11. Sandbox / dev environment quirks

- `h11` is sandbox-blocked → uvicorn must run with `httptools` backend.
- Dev server: `dev_server.py`.
- Playwright smoke suite: `pytest tests/test_smoke.py`.
- GitHub Actions CI runs the smoke suite on push.
- Auto cache-buster (mtime hash of `assets/`) means hard-refresh isn't needed in dev.

---

## 12. How to pick up next session

1. Read this file.
2. `git log --oneline -10` to see what landed since this brain was last consolidated.
3. If the user asks about a specific panel, grep `dashboard.html` for the section ID and trace: HTML hook → `hydrate.js` writer → backend route → `data_service.py` fetcher.
4. Don't add UI band-aids when the fix belongs in the data layer. Filter at the dataset.
5. Update this file when architectural decisions change.
