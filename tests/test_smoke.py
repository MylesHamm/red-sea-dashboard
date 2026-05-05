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
    """Every advertised KPI should contain a real value, not '—'."""
    val = _wait_for_kpi(page, kpi_selector, timeout=15_000)
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


def test_chokepoint_cards_have_recent_events(page: Page):
    """Each chokepoint card's INCIDENTS · 30D should reflect recent activity.

    With the merged-pool keyword scope, both Hormuz and Bab should report a
    non-zero count (overlapping Iran-war news matches both). If either
    collapses to 0 here under the active war, the keyword filter or the
    merged-pool path has regressed. Cape is the safe alternate route and
    legitimately reports 0 (intentionally hardcoded). Suez does NOT have
    a card in §02 — it lives only in the active-target sidebar.
    """
    page.click('.nav-item[data-tab="geospatial"]')
    page.wait_for_timeout(2_000)
    for cp in ("hormuz", "bab"):
        el = page.query_selector(f'[data-cp-card="{cp}"] [data-stat="incidents"]')
        assert el is not None, f"{cp} card not found in DOM"
        val = (el.text_content() or "").strip()
        try:
            n = int(val)
        except ValueError:
            pytest.fail(f"{cp} card's INCIDENTS · 30D is not numeric: {val!r}")
        assert n > 0, f"{cp} card shows {n} incidents — keyword filter may have regressed"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
