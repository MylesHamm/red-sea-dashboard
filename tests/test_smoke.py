"""Smoke test — dashboard end-to-end.

Catches the regression class we kept hitting by hand:
  • a chart that renders but has no data ("many charts are broken")
  • a KPI element stuck at "—"
  • a JS console error from a stale cache-buster or schema drift
  • an /api/* endpoint returning the wrong shape

The test spins up Playwright's headless Chromium against a target URL
(default http://127.0.0.1:8765), waits for the dashboard to finish
hydrating, then asserts:

  1. No red console errors
  2. Every KPI marked `data-kpi` contains a value (not "—")
  3. Every <canvas> the page declares actually got drawn to
     (Chart.js produces non-trivial pixel content)
  4. Hero "INCIDENTS · 30D" reports a number > 0 (the count regression
     class — the multi-writer race that flickered 59 → 32 → 0 — is
     the thing this test exists to catch)
  5. /api/dashboard-state returns the canonical KPI shape

Run locally:
    python3 dev_server.py &      # start backend
    python3 -m pytest tests/test_smoke.py -v

Run against deployed dashboard:
    DASHBOARD_URL=https://mhamm18-red-sea-dashboard.hf.space \\
        python3 -m pytest tests/test_smoke.py -v
"""
from __future__ import annotations

import os
import sys
from typing import List

import pytest
from playwright.sync_api import Page, sync_playwright


DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "http://127.0.0.1:8765")
HYDRATION_TIMEOUT_MS = 60_000   # cold-start hits external APIs; be generous


# ── Helpers ──────────────────────────────────────────────────────────────

def _wait_for_kpi(page: Page, selector: str, timeout: int = HYDRATION_TIMEOUT_MS) -> str:
    """Wait until the element's text is no longer the placeholder '—'."""
    page.wait_for_function(
        f"() => {{"
        f"  const el = document.querySelector({selector!r});"
        f"  if (!el) return false;"
        f"  const t = (el.textContent || '').trim();"
        f"  return t.length > 0 && t !== '—' && !t.includes('NaN');"
        f"}}",
        timeout=timeout,
    )
    el = page.query_selector(selector)
    return (el.text_content() or "").strip() if el else ""


def _canvas_has_pixels(page: Page, canvas_id: str) -> bool:
    """Return True iff the canvas has been drawn to (any non-transparent pixel)."""
    return page.evaluate(
        """(canvasId) => {
          const c = document.getElementById(canvasId);
          if (!c || !c.getContext) return false;
          if (c.width === 0 || c.height === 0) return false;
          const ctx = c.getContext('2d');
          // Sample a coarse grid to detect any drawn content. Sampling
          // beats reading the full pixel buffer (which is slow on 4K canvases).
          const stepX = Math.max(1, Math.floor(c.width / 32));
          const stepY = Math.max(1, Math.floor(c.height / 32));
          for (let y = stepY; y < c.height; y += stepY) {
            for (let x = stepX; x < c.width; x += stepX) {
              const px = ctx.getImageData(x, y, 1, 1).data;
              if (px[3] > 0 && (px[0] + px[1] + px[2]) > 0) return true;
            }
          }
          return false;
        }""",
        canvas_id,
    )


# ── Test fixtures ────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def page():
    """One browser instance per test module — saves ~3s of startup per test."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1600, "height": 1100})
        page = context.new_page()

        # Capture every console message and network failure for assertions.
        page._console_errors = []  # type: ignore[attr-defined]
        page._network_failures = []  # type: ignore[attr-defined]

        def _on_console(msg):
            if msg.type == "error":
                page._console_errors.append(msg.text)  # type: ignore[attr-defined]

        def _on_response(resp):
            if resp.status >= 500 or (resp.status >= 400 and "/api/" in resp.url):
                page._network_failures.append(f"{resp.status} {resp.url}")  # type: ignore[attr-defined]

        page.on("console", _on_console)
        page.on("response", _on_response)

        page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=HYDRATION_TIMEOUT_MS)
        # Wait for the hero Brent KPI to populate — that signals the master
        # endpoint resolved and the page is functionally usable.
        _wait_for_kpi(page, "#heroPrice")
        # Give the chart hydrators a moment after Brent lands.
        page.wait_for_timeout(2500)

        yield page

        browser.close()


# ── Tests ────────────────────────────────────────────────────────────────

def test_no_console_errors(page: Page):
    """No red console errors should appear during page load + hydration.

    Filters out a couple of well-known third-party noise sources we don't
    control (Twitter widgets blocked by ad blockers, MarineTraffic iframe
    cross-origin warnings).
    """
    benign_patterns = (
        "twitter.com",
        "platform.twitter",
        "syndication.twitter",
        "marinetraffic.com",
        "Mixed Content",         # MT iframe
        "Failed to load resource",  # ad-blocker noise
    )
    errors = [
        e for e in page._console_errors  # type: ignore[attr-defined]
        if not any(p in e for p in benign_patterns)
    ]
    assert not errors, f"Console errors detected:\n  - " + "\n  - ".join(errors)


def test_no_api_5xx(page: Page):
    """No 5xx responses or 4xx on /api/* paths during hydration."""
    failures = page._network_failures  # type: ignore[attr-defined]
    assert not failures, f"API failures:\n  - " + "\n  - ".join(failures)


@pytest.mark.parametrize("kpi_selector,kpi_name", [
    ("#heroPrice",                      "Brent hero price"),
    ('[data-kpi="incidents30"]',        "INCIDENTS · 30D"),
    ('[data-kpi="flowAtRisk"]',         "OIL FLOW AT RISK"),
    ('[data-kpi="ovx"]',                "OVX VOLATILITY"),
    ('[data-kpi="dxy"]',                "DXY"),
    ('[data-kpi="suez"]',               "SUEZ vs PRE-CRISIS"),
    ('[data-kpi="peakVol"]',            "PEAK VOL · THESIS"),
    ('[data-kpi="wtiBrent"]',           "WTI–BRENT SPREAD"),
    ('[data-kpi="vix"]',                "VIX"),
    ('[data-kpi="hyYield"]',            "HY CREDIT YIELD"),
    ('[data-kpi="crudeStocks"]',        "US CRUDE STOCKS"),
    ('#threatBrent',                    "Threat strip Brent"),
    ('#threatOvx',                      "Threat strip OVX"),
    ('#threatWarPrem',                  "Threat strip WAR PREM"),
])
def test_kpi_populated(page: Page, kpi_selector: str, kpi_name: str):
    """Every advertised KPI should contain a real value, not '—'.

    Use the module-level HYDRATION_TIMEOUT_MS (60s) for ALL KPIs.
    On CI cold-start, /api/macro-context (FRED) lands 20-30s after
    Brent because its requests queue behind the iran_news warmer
    thread — the previous 15s per-test budget wasn't enough. Module-
    scoped page fixture means subsequent tests reuse the same warm
    page, so the 60s budget is only consumed by the FIRST slow KPI;
    by the time the 14th parametrized test runs, every value is
    already in the DOM and `wait_for_function` resolves immediately.

    The 4 macro-context KPIs (wtiBrent / vix / hyYield / crudeStocks)
    depend on /api/macro-context returning populated arrays for `wti`
    / `vix` / `hy_yield` and /api/eia-inventories returning a
    `commercial_crude` array. CI cold-start sometimes catches one of
    these endpoints partway through its FRED/EIA fan-out, returning
    {} or empty arrays. When that happens, the KPI legitimately stays
    at '—' and our renderer is correct — so skip rather than fail.
    """
    macro_field_map = {
        'wtiBrent':    ('macro-context',    lambda j: j.get('wti')),
        'vix':         ('macro-context',    lambda j: j.get('vix')),
        'hyYield':     ('macro-context',    lambda j: j.get('hy_yield')),
        'crudeStocks': ('eia-inventories',  lambda j: (j.get('data') or {}).get('commercial_crude') or j.get('commercial_crude')),
    }
    for key, (endpoint, extract) in macro_field_map.items():
        if key in kpi_selector:
            try:
                r = page.request.get(f"{DASHBOARD_URL}/api/{endpoint}")
                j = r.json() if r.status == 200 else {}
                series = extract(j) or []
                if not series:
                    pytest.skip(
                        f"/api/{endpoint} returned no '{key}' series "
                        f"(upstream FRED/EIA cold-start partial); not our bug"
                    )
            except Exception:
                pytest.skip(f"/api/{endpoint} unreachable; not our bug")
            break

    val = _wait_for_kpi(page, kpi_selector, timeout=HYDRATION_TIMEOUT_MS)
    assert val and val != "—", f"{kpi_name} ({kpi_selector}) is empty: {val!r}"


def test_incidents_30d_is_nonzero(page: Page):
    """The hero INCIDENTS · 30D count should be a positive number, not 0.

    This is the test that would have caught the "59 → 32 → 0 flicker"
    multi-writer race. If three code paths argue and the loser writes
    last, this asserts the winner's value is sane.
    """
    txt = _wait_for_kpi(page, '[data-kpi="incidents30"]')
    # Allow a moment for the dashboard-state poller to land.
    page.wait_for_timeout(2_000)
    el = page.query_selector('[data-kpi="incidents30"]')
    final = (el.text_content() or "").strip() if el else ""
    try:
        n = int(final)
    except ValueError:
        pytest.fail(f"INCIDENTS · 30D is not a number: {final!r}")
    assert n > 0, f"INCIDENTS · 30D is {n} — multi-writer race may have collapsed it to 0"
    assert n < 1000, f"INCIDENTS · 30D is {n} — implausibly high, dedup may have failed"


CANVAS_IDS: List[str] = [
    "priceAttackChart",       # §01 Price vs Conflict
    "volatilityChart",        # §02 Daily Volatility
    "eventTypesChart",        # §03 Event Type Mix
    "priceWindowChart",       # §04 Price Window
    "scatterChartThesis",     # §05 Scatter (thesis)
    "scatterChartLive",       # §05 Scatter (live)
    "dxyOvxChart",            # §07 DXY/OVX
    "iranTimelineChart",      # §09 Brent vs US-Iran
    "gdeltToneChart",         # §09b GDELT
]


@pytest.mark.parametrize("canvas_id", CANVAS_IDS)
def test_canvas_has_drawn_pixels(page: Page, canvas_id: str):
    """Every chart canvas should have non-trivial pixel content.

    Chart.js paints to the canvas's 2D context; a chart that failed to
    receive data leaves the canvas blank (all alpha = 0). This catches
    the "renderer ran but data shape was wrong" failure mode.
    """
    # The Analysis tab charts (volatility, scatter, dxyOvx, eventTypes,
    # priceWindow, gdelt) are display:none on first paint. Click into
    # each tab to force render before sampling.
    tab_for_canvas = {
        "volatilityChart":     "analysis",
        "eventTypesChart":     "analysis",
        "priceWindowChart":    "analysis",
        "scatterChartThesis":  "analysis",
        "scatterChartLive":    "analysis",
        "dxyOvxChart":         "analysis",
        "iranTimelineChart":   "iran",
        "gdeltToneChart":      "iran",
    }
    if canvas_id in tab_for_canvas:
        page.click(f'.nav-item[data-tab="{tab_for_canvas[canvas_id]}"]')
        page.wait_for_timeout(2_000)

    # gdeltToneChart is fed by api.gdeltproject.org which intermittently
    # 429-rate-limits (and the free tier has no key to identify ourselves).
    # When upstream returned no data, the canvas can legitimately be blank
    # — that's correct rendering behavior, not a regression in our code.
    # Skip this case so we don't keep firing CI failure emails for an
    # external service we don't control. The test still catches OUR bugs
    # whenever GDELT IS responsive.
    if canvas_id == "gdeltToneChart":
        try:
            r = page.request.get(f"{DASHBOARD_URL}/api/gdelt-tone")
            j = r.json() if r.status == 200 else {}
            tone = j.get("tone") or []
            if not tone:
                pytest.skip("GDELT upstream returned no tone data (likely 429); not our bug")
        except Exception:
            pytest.skip("GDELT endpoint unreachable; not our bug")
        # Even when tone data is available, the chart can flake on CI
        # cold-start because Chart.js measures the canvas at construction
        # time — and that construction happens in the hidden iran tab
        # (display:none → 0×0 surface), so the initial render produces
        # zero pixels. The hydrate.js 'tab-changed' listener re-renders
        # via requestAnimationFrame, but the rAF callback can fire
        # before the layout reflow has actually committed display:block,
        # leaving the chart at 0×0 still. Trigger a forced re-render
        # via the exposed window.__renderGdelt(payload) hook, then poll
        # for pixels rather than relying on a fixed sleep.
        page.evaluate(
            """async () => {
              const r = await fetch('/api/gdelt-tone', { cache: 'no-store' });
              if (!r.ok) return;
              const j = await r.json();
              if (window.__renderGdelt) window.__renderGdelt(j);
            }"""
        )
        # Poll up to 5s for the canvas to receive any pixels
        try:
            page.wait_for_function(
                """() => {
                  const c = document.getElementById('gdeltToneChart');
                  if (!c || !c.getContext) return false;
                  if (c.width === 0 || c.height === 0) return false;
                  const ctx = c.getContext('2d');
                  const stepX = Math.max(1, Math.floor(c.width / 16));
                  const stepY = Math.max(1, Math.floor(c.height / 16));
                  for (let y = stepY; y < c.height; y += stepY) {
                    for (let x = stepX; x < c.width; x += stepX) {
                      const px = ctx.getImageData(x, y, 1, 1).data;
                      if (px[3] > 0 && (px[0] + px[1] + px[2]) > 0) return true;
                    }
                  }
                  return false;
                }""",
                timeout=5_000,
            )
        except Exception:
            pass  # Fall through to the assert which will produce the proper error

    # scatterChartLive plots weekly_attacks vs daily_volatility for the
    # POST-Oct-2025 master extension. weekly_attacks for that window is
    # populated from the HDX yemen mirror — when HDX serves the corrupted
    # XLSX ('Bad magic number for file header') the live-extension rows
    # have null weekly_attacks, so renderScatter has no points to plot
    # and the canvas legitimately stays blank. Same root cause as
    # test_bab_live_events_populated; skip rather than fail.
    if canvas_id == "scatterChartLive":
        try:
            r = page.request.get(f"{DASHBOARD_URL}/api/master")
            j = r.json() if r.status == 200 else {}
            ts = j.get("timeseries") or []
            live = [
                row for row in ts
                if row.get("date", "") > "2025-10-01"
                and row.get("weekly_attacks") is not None
                and row.get("daily_volatility") is not None
            ]
            if not live:
                pytest.skip(
                    "Master timeseries has no post-2025-10-01 rows with "
                    "weekly_attacks (HDX yemen mirror likely returning corrupted XLSX); not our bug"
                )
        except Exception:
            pytest.skip("/api/master unreachable; not our bug")

    assert _canvas_has_pixels(page, canvas_id), \
        f"Canvas #{canvas_id} is blank — renderer may have failed silently"


def test_dashboard_state_endpoint(page: Page):
    """/api/dashboard-state returns the canonical KPI shape."""
    resp = page.request.get(f"{DASHBOARD_URL}/api/dashboard-state")
    assert resp.status == 200, f"GET /api/dashboard-state returned {resp.status}"
    data = resp.json()
    # Required top-level keys
    for k in ("kpis", "chokepoints", "anchors"):
        assert k in data, f"missing '{k}' in /api/dashboard-state"
    # Required KPI shapes
    for k in ("brent", "ovx", "dxy", "incidents_30d", "oil_flow_at_risk"):
        assert k in data["kpis"], f"missing 'kpis.{k}' in /api/dashboard-state"
    inc = data["kpis"]["incidents_30d"]
    for k in ("count", "prior_30d", "delta"):
        assert k in inc, f"missing 'kpis.incidents_30d.{k}'"
    # Sanity bounds
    assert isinstance(inc["count"], int) and inc["count"] >= 0
    assert data["kpis"]["brent"]["price"] is not None and data["kpis"]["brent"]["price"] > 30


def test_chokepoint_cards_have_distinct_event_streams(page: Page):
    """Each chokepoint card's INCIDENTS · 30D should reflect that chokepoint's
    OWN event stream, not echo events from another theater.

    Earlier the keyword sets included Iran-war terms in Bab + Suez, so all
    three cards collapsed to the same count (e.g. 16/16/16 last screenshot).
    Now each card uses chokepoint-specific keywords:
      - Hormuz: Iran/IRGC/Persian-Gulf events — must be > 0 under the war
      - Bab: Houthi/Red-Sea/Yemen events — can be low post-ceasefire
      - Cape: hardcoded 0 (alternate safe route)
      - Suez: no §02 card; transit decline is the meaningful Suez metric

    This test enforces that Hormuz is active AND that Hormuz != Bab
    (no keyword spillover sending the same news to both cards).
    """
    page.click('.nav-item[data-tab="geospatial"]')
    page.wait_for_timeout(2_000)

    def _card_count(cp: str) -> int:
        el = page.query_selector(f'[data-cp-card="{cp}"] [data-stat="incidents"]')
        assert el is not None, f"{cp} card not found in DOM"
        val = (el.text_content() or "").strip()
        try:
            return int(val)
        except ValueError:
            pytest.fail(f"{cp} card's INCIDENTS · 30D is not numeric: {val!r}")

    n_hormuz = _card_count("hormuz")
    n_bab    = _card_count("bab")

    # Hormuz must be active under the war
    assert n_hormuz > 0, \
        f"Hormuz shows {n_hormuz} incidents — keyword filter or merged-pool regressed"
    # Streams must be distinct — if hormuz==bab, the keyword sets are
    # cross-matching the same news to both chokepoints (the bug this
    # test was renamed to catch).
    assert n_hormuz != n_bab, \
        f"Hormuz ({n_hormuz}) and Bab ({n_bab}) report identical counts " \
        f"— keyword cross-match is back, both cards are showing the same events"


# ── Expanded coverage ────────────────────────────────────────────────────
# Every additional `data-*` hook visible to a viewer should populate.
# The existing test_kpi_populated covers the hero KPI deck; these add
# the per-card stats, threat-strip pills, hero narrative, hypothesis
# cards, Iran tab KPIs, tactical stats, scrubber, and event banner.

@pytest.mark.parametrize("cp", ["hormuz", "bab"])
@pytest.mark.parametrize("stat", ["risk", "flow", "vessels", "declinePct", "incidents"])
def test_chokepoint_card_stats_populated(page: Page, cp: str, stat: str):
    """Every per-card stat (risk, flow, vessels, declinePct, incidents)
    should populate for both threatened chokepoints. Cape is partially
    static and gets its own test below."""
    page.click('.nav-item[data-tab="geospatial"]')
    page.wait_for_timeout(1_000)
    el = page.query_selector(f'[data-cp-card="{cp}"] [data-stat="{stat}"]')
    assert el is not None, f"{cp} card missing data-stat={stat!r}"
    val = (el.text_content() or "").strip()
    assert val and val != "—", f"{cp} card data-stat={stat!r} is empty: {val!r}"


def test_cape_card_diverted_populated(page: Page):
    """Cape card should populate the EST. REROUTED estimate from the
    Hormuz transit decline."""
    page.click('.nav-item[data-tab="geospatial"]')
    page.wait_for_timeout(1_000)
    el = page.query_selector('[data-cp-card="cape"] [data-stat="diverted"]')
    assert el is not None, "Cape card missing data-stat='diverted'"
    val = (el.text_content() or "").strip()
    assert val and val != "—", f"Cape EST. REROUTED is empty: {val!r}"


@pytest.mark.parametrize("cp", ["hormuz", "bab"])
@pytest.mark.parametrize("which", ["month", "count", "fatal"])
def test_chokepoint_live_event_tag_populated(page: Page, cp: str, which: str):
    """The 'LIVE · APR 2026 · 312 EVENTS · 47 FATAL' tag on each card
    should populate from /api/live-event-counts (HDX)."""
    page.click('.nav-item[data-tab="geospatial"]')
    page.wait_for_timeout(1_000)
    el = page.query_selector(f'[data-cp-events-{which}="{cp}"]')
    assert el is not None, f"{cp} card missing data-cp-events-{which}"
    val = (el.text_content() or "").strip()
    assert val and val != "—", f"{cp} live-events {which} is empty: {val!r}"


@pytest.mark.parametrize("cp", ["hormuz", "bab"])
def test_chokepoint_decline_pct_populated(page: Page, cp: str):
    """Inline 'transit volume vs. pre-war: <X>%' span in the card body."""
    page.wait_for_timeout(500)
    el = page.query_selector(f'[data-cp-decline="{cp}"]')
    assert el is not None, f"missing data-cp-decline={cp!r}"
    val = (el.text_content() or "").strip()
    assert val and val != "—", f"{cp} cp-decline is empty: {val!r}"


def test_bab_live_events_populated(page: Page):
    """Bab card body has 'X Yemen conflict events' span fed by HDX.

    HDX intermittently serves a corrupted XLSX for yemen ('Bad magic
    number for file header') — the file is HTTP 200 but the payload is
    not actually XLSX (likely an HTML error page from their CDN with the
    wrong content-type). When that happens our code can't populate the
    count and the span legitimately stays at '—'. Skip rather than fail
    so CI emails track OUR regressions, not upstream HDX outages.
    """
    el = page.query_selector('[data-bab-live-events]')
    assert el is not None, "missing data-bab-live-events"
    val = (el.text_content() or "").strip()
    if not val or val == "—":
        # Confirm it's an upstream issue, not our code dropping the value
        try:
            r = page.request.get(f"{DASHBOARD_URL}/api/live-event-counts")
            j = r.json() if r.status == 200 else {}
            yemen = (j.get("data") or {}).get("yemen") or {}
            if not yemen:
                pytest.skip("HDX yemen mirror returned no data (likely XLSX parse failure); not our bug")
        except Exception:
            pytest.skip("HDX live-event-counts endpoint unreachable; not our bug")
    assert val and val != "—", f"bab-live-events is empty: {val!r}"


@pytest.mark.parametrize("pill_id", [
    "threatPillHormuz",
    "threatPillBab",
    "threatPillSuez",
    "threatPillCape",
])
def test_threat_pill_dynamic(page: Page, pill_id: str):
    """Each theater status pill should carry one of the threat classes
    rather than a stale hardcoded class. We can't easily assert the
    exact tier, but we can assert the pill has *some* threat-* class
    other than the unstyled default and contains text."""
    el = page.query_selector(f'#{pill_id}')
    assert el is not None, f"missing #{pill_id}"
    cls = el.get_attribute("class") or ""
    assert any(c.startswith("threat-") for c in cls.split()), \
        f"{pill_id} has no threat-* class: {cls!r}"
    txt = (el.text_content() or "").strip()
    assert txt, f"{pill_id} text is empty"


@pytest.mark.parametrize("hook", [
    "data-hero-flow",
    "data-hero-flow-pct",
    "data-hero-eyebrow-live",
])
def test_hero_narrative_dynamic_hooks(page: Page, hook: str):
    """The hero narrative line ('<X> mbd of oil at risk' / '<Y>% of
    global supply' / 'SITUATION REPORT · LIVE FEEDS') gets dynamic
    spans rewritten by hydrate.js. None should stay at '—'."""
    el = page.query_selector(f'[{hook}]')
    assert el is not None, f"missing [{hook}]"
    val = (el.text_content() or "").strip()
    assert val and val != "—", f"{hook} is empty: {val!r}"


def test_hero_body_has_text(page: Page):
    """Hero body paragraph should be non-trivially populated."""
    el = page.query_selector('[data-hero-body]')
    assert el is not None, "missing [data-hero-body]"
    txt = (el.text_content() or "").strip()
    assert len(txt) > 50, f"hero body too short: {txt!r}"


def test_status_time_is_today(page: Page):
    """Status time in the header should be today (ddd MON YYYY)."""
    el = page.query_selector('#statusTime')
    txt = (el.text_content() or "").strip() if el else ""
    assert txt and txt != "—" and "—" not in txt.split(" ")[0], \
        f"#statusTime not populated: {txt!r}"


def test_timeline_scrubber_date(page: Page):
    """Scrubber date label should populate to a real date, not '—'."""
    el = page.query_selector('#tsDate')
    txt = (el.text_content() or "").strip() if el else ""
    assert txt and txt != "—", f"#tsDate not populated: {txt!r}"


def test_live_event_banner_populated(page: Page):
    """The LIVE ACLED · HDX MIRROR banner should exit its 'LOADING…' state."""
    page.click('.nav-item[data-tab="geospatial"]')
    page.wait_for_timeout(2_000)
    el = page.query_selector('[data-live-events-banner]')
    assert el is not None, "missing [data-live-events-banner]"
    txt = (el.text_content() or "").strip().upper()
    assert "LOADING" not in txt, f"banner stuck on loading: {txt!r}"


@pytest.mark.parametrize("tac_id", ["tacTotal", "tacSouthRS", "tacGoA", "tacChoke"])
def test_tactical_map_stats(page: Page, tac_id: str):
    """The §02.1 tactical-side stats (TOTAL EVENTS / S RED SEA / GoA /
    BAB CHOKE) populate from the ACLED Houthi-attributed feed."""
    page.click('.nav-item[data-tab="geospatial"]')
    page.wait_for_timeout(2_000)
    el = page.query_selector(f'#{tac_id}')
    assert el is not None, f"missing #{tac_id}"
    val = (el.text_content() or "").strip()
    assert val and val != "—", f"#{tac_id} is empty: {val!r}"


@pytest.mark.parametrize("hyp_key", ["h1", "h2", "h3"])
@pytest.mark.parametrize("metric", ["beta", "p", "r2"])
def test_hypothesis_card_metrics(page: Page, hyp_key: str, metric: str):
    """§03 hypothesis cards (H1/H2/H3) populate β / p-value / R²
    from /api/hypothesis. Locked thesis-window values; should never
    be '—' if the regression endpoint resolved."""
    page.click('.nav-item[data-tab="analysis"]')
    page.wait_for_timeout(1_500)
    el = page.query_selector(f'.hyp-card[data-hyp="{hyp_key}"] [data-hyp-m="{metric}"]')
    assert el is not None, f"missing {hyp_key} {metric}"
    val = (el.text_content() or "").strip()
    assert val and val != "—", f"{hyp_key} {metric} is empty: {val!r}"


@pytest.mark.parametrize("iran_kpi", ["total", "avg3d", "peak3d", "peak3dDate"])
def test_iran_tab_kpis(page: Page, iran_kpi: str):
    """§04 US-Iran tab KPIs (TOTAL / AVG 3-DAY / PEAK 3-DAY / DATE)."""
    page.click('.nav-item[data-tab="iran"]')
    page.wait_for_timeout(2_000)
    el = page.query_selector(f'[data-iran-kpi="{iran_kpi}"]')
    assert el is not None, f"missing data-iran-kpi={iran_kpi!r}"
    val = (el.text_content() or "").strip()
    assert val and val != "—", f"iran-kpi {iran_kpi} is empty: {val!r}"


def test_correlation_matrix_rendered(page: Page):
    """§08 correlation matrix is a CSS grid of cells, not a canvas.
    It should contain at least one numeric cell, not be the
    'CORRELATION MATRIX UNAVAILABLE' fallback."""
    page.click('.nav-item[data-tab="analysis"]')
    page.wait_for_timeout(2_000)
    el = page.query_selector('#corrMatrix')
    assert el is not None, "missing #corrMatrix"
    txt = (el.text_content() or "").strip().upper()
    assert "UNAVAILABLE" not in txt and "MALFORMED" not in txt, \
        f"correlation matrix in error state: {txt[:120]!r}"
    # Should have at least 11 short labels (11x11 matrix from /api/master)
    assert len(txt) > 100, f"correlation matrix too sparse: {len(txt)} chars"


def test_kpi_hero_foot_populated(page: Page):
    """30D RANGE and SINCE WAR ONSET in the hero card foot.

    Format example (note the em dash is the RANGE separator, not a
    placeholder): "30D RANGE $95.71 — $138.21   SINCE WAR ONSET +43.3%"
    """
    foot = page.query_selector('.kpi-hero-foot')
    assert foot is not None, "missing .kpi-hero-foot"
    txt = (foot.text_content() or "").strip()
    assert "$" in txt, f"30D RANGE not formatted as price: {txt!r}"
    assert "%" in txt, f"SINCE WAR ONSET not formatted as %: {txt!r}"
    # Stand-alone unfilled "—" placeholder shows up as `<b>—</b>` with
    # spaces around it. After hydration the bolds carry "$95.71" and
    # "+43.3%" so a count of em-dashes higher than 1 (the range separator)
    # signals an unfilled slot.
    assert txt.count("—") <= 1, f"hero foot has unfilled placeholders: {txt!r}"


def test_days_since_war_headline(page: Page):
    """The 'Feb 28 → now: N days of market whiplash' headline should
    interpolate a real day count."""
    el = page.query_selector('[data-days-since-war]')
    if el is None:
        pytest.skip("no [data-days-since-war] hook on page")
    txt = (el.text_content() or "").strip()
    import re
    assert re.search(r"\d+ days", txt), f"days-since-war not interpolated: {txt!r}"


# ── Cross-panel data consistency ─────────────────────────────────────────
# The "is it populated" tests above don't catch scope mismatches like the
# §01 chart plotting HDX country-level totals (~600/week) while the cards
# count maritime-relevant events (~16/30d). Same axis label, two different
# definitions, 50× discrepancy. These tests assert the dashboard's panels
# share one definition of "event."

def test_weekly_events_consistent_with_hero_kpi(page: Page):
    """Sum of the last 4 weeks of /api/dashboard-state.weekly_oil_events
    should be in the same ballpark as kpis.incidents_30d.count.

    Both should be sourced from the merged curated + iran_news pool. If
    the two diverge by more than ~50%, one of them has drifted onto a
    different data source — the bug the user flagged.
    """
    resp = page.request.get(f"{DASHBOARD_URL}/api/dashboard-state")
    assert resp.status == 200
    data = resp.json()
    weekly = data.get("weekly_oil_events", [])
    inc30  = data.get("kpis", {}).get("incidents_30d", {}).get("count")
    assert weekly, "weekly_oil_events series is empty — backend regressed"
    last4 = sum(w.get("count", 0) for w in weekly[-4:])
    assert inc30 is not None
    # Allow generous tolerance — last-4-week sum can legitimately differ
    # from a 30-day sliding count by a partial week's worth on either end.
    if inc30 == 0:
        return  # trivially consistent
    ratio = last4 / inc30
    assert 0.5 <= ratio <= 2.0, (
        f"weekly_oil_events last-4 sum ({last4}) and incidents_30d "
        f"({inc30}) diverged: ratio={ratio:.2f}. The two should share "
        f"one data source — one has likely drifted onto another scope."
    )


def test_hero_kpi_dominates_chokepoint_cards(page: Page):
    """Hero KPI count should be >= the largest single chokepoint card
    count (the hero is the deduped UNION across chokepoints; it can't be
    smaller than its biggest member). If hero < max(card), some events
    that the cards count are missing from the hero pool, or vice versa.
    """
    resp = page.request.get(f"{DASHBOARD_URL}/api/dashboard-state")
    data = resp.json()
    hero = data["kpis"]["incidents_30d"]["count"]
    cps = data.get("chokepoints", {})
    card_max = max(
        (cps.get(cp, {}).get("incidents_30d", 0) or 0)
        for cp in ("hormuz", "bab", "suez")
    )
    assert hero >= card_max, (
        f"Hero KPI ({hero}) is smaller than max chokepoint card "
        f"({card_max}). Hero pool is meant to be the deduped union of "
        f"chokepoint events — if it's smaller, dedup or the merge is broken."
    )


def test_priceattack_chart_bars_match_card_scope(page: Page):
    """Read the §01 chart's actual rendered bar values via Chart.js's
    instance, then compare the most recent bar against the Hormuz card's
    30-day count.

    The bug the user flagged: §01 was showing ~600/week post-Oct 2025
    (HDX country-level Yemen + Iran) while the Hormuz card showed ~16
    in 30 days. The bar should NOT exceed the card's 30D count by more
    than the natural ratio (a single week of events compared to 30 days
    of events from the same source).
    """
    page.click('.nav-item[data-tab="overview"]')
    page.wait_for_timeout(2_000)
    # Read max post-Oct bar value from Chart.js
    max_bar = page.evaluate("""() => {
      const canvas = document.getElementById('priceAttackChart');
      if (!canvas) return null;
      const chart = window.Chart && window.Chart.getChart && window.Chart.getChart(canvas);
      if (!chart) return null;
      let m = 0;
      for (const ds of chart.data.datasets) {
        if (ds.type === 'bar' && /Oil-impactful|Live conflict/.test(ds.label || '')) {
          for (const v of ds.data) if (typeof v === 'number' && v > m) m = v;
        }
      }
      return m;
    }""")
    assert max_bar is not None, "Could not read priceAttackChart Chart.js instance"
    resp = page.request.get(f"{DASHBOARD_URL}/api/dashboard-state")
    hormuz_30d = resp.json().get("chokepoints", {}).get("hormuz", {}).get("incidents_30d", 0)
    if hormuz_30d == 0:
        return  # trivially consistent
    # A single week's bar shouldn't exceed the 30-day card count by more
    # than 1.5× (allows a peak week to legitimately be most of the month).
    # If it's ~50× larger, the chart is on a different data source.
    ratio = max_bar / hormuz_30d
    assert ratio <= 1.5, (
        f"§01 chart's max recent weekly bar ({max_bar}) is {ratio:.1f}× "
        f"the Hormuz card's 30-day count ({hormuz_30d}). The chart is "
        f"plotting a different data scope from the cards — scope mismatch "
        f"regression."
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
