/* ═══════════════════════════════════════════════════════════════════════════
   CHOKEPOINT INTEL — Hydrator
   ───────────────────────────────────────────────────────────────────────────
   Pulls live data from the backend (/api/*) and:
     1. Populates the globe / map data stores (CHOKEPOINTS, ATTACKS, etc.)
     2. Updates KPI DOM elements (hero price, OVX, threats)
     3. Pushes the Brent sparkline, event feed, news feed
     4. Exposes CP.hydrated → a Promise that resolves once the first load
        is complete, so chart code can await it.
   ───────────────────────────────────────────────────────────────────────────
   No hardcoded analytical numbers live here. If a value cannot be
   resolved from the backend, the DOM shows "—" (em-dash) rather than a fake
   number.  The connection indicator in the header turns green only after a
   successful /api/health response.
   ═══════════════════════════════════════════════════════════════════════════ */

(function () {
  'use strict';

  const U = window.APIUtil;
  const API = window.API;
  if (!API) { console.error('API client not loaded'); return; }

  // ── DOM helpers ───────────────────────────────────────────────────────────
  const $ = id => document.getElementById(id);
  const setText = (id, t) => { const el = $(id); if (el) el.textContent = t; };
  const setHTML = (id, h) => { const el = $(id); if (el) el.innerHTML = h; };

  function setDelta(el, n, { suffix = '', decimals = 2, reverse = false } = {}) {
    if (!el) return;
    if (n == null || isNaN(n)) { el.textContent = '—'; el.className = el.className.replace(/\b(delta-up|delta-down|up|down)\b/g, ''); return; }
    const up = reverse ? n < 0 : n > 0;
    el.textContent = (n > 0 ? '▲ +' : n < 0 ? '▼ ' : '') + Math.abs(n).toFixed(decimals) + suffix;
    el.classList.remove('delta-up', 'delta-down', 'up', 'down');
    el.classList.add(up ? 'delta-up' : 'delta-down');
  }

  // ── Connection indicator ──────────────────────────────────────────────────
  // severity: 'live' (default when ok), 'warn' (some sources stale), 'err'.
  // Three tiers so DEGRADED looks different from full OFFLINE.
  function setConnectionStatus(ok, label, severity) {
    const dot = document.querySelector('.status-dot');
    const txt = document.querySelector('.status-text');
    const sev = severity || (ok ? 'live' : 'err');
    if (dot) {
      dot.classList.remove('live', 'warn', 'err');
      dot.classList.add(sev);
    }
    if (txt) {
      txt.classList.remove('warn', 'err');
      if (sev !== 'live') txt.classList.add(sev);
      txt.textContent = label || (ok ? 'LIVE · ACLED + EIA' : 'OFFLINE · using cache');
    }
  }

  // ── Per-source freshness pill + chokepoint card "data through" captions ──
  // Backed by /api/freshness, polled every 60s. The endpoint returns the
  // *newest event date* (not last-fetch ts) for every source, plus an
  // aggregate `status.level` of "live" | "stale" | "frozen". The pill text
  // and dot color are derived from that — no hardcoded "LIVE" claim.
  // Surfaces a per-card caption (e.g. "EVENTS THROUGH MAR 2025 · ACLED
  // FALLBACK") so users see exactly which window they're looking at.
  const FR_MONTH_ABBR = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
  function _frFmtDate(iso) {
    if (!iso || iso.length < 10) return '—';
    const [y, m, d] = iso.split('-').map(s => parseInt(s, 10));
    if (!y || !m) return iso;
    return `${String(d || 1).padStart(2,'0')} ${FR_MONTH_ABBR[m-1]} ${y}`;
  }
  function _frFmtMonth(ym) {
    if (!ym || ym.length < 7) return '—';
    const [y, m] = ym.split('-').map(s => parseInt(s, 10));
    return (m && y) ? `${FR_MONTH_ABBR[m-1]} ${y}` : ym;
  }
  function hydrateFreshness(snap) {
    if (!snap || typeof snap !== 'object') return;
    window.FRESHNESS = snap;
    // Hero eyebrow gets a live-source breakdown derived from the same
    // snapshot so users see "🟢 MARKETS LIVE THROUGH 02 MAY 2026" instead
    // of the old static "UPDATED HOURLY" text (which was both vague and
    // sometimes wrong since some feeds update every 4h, others every 5min).
    try { hydrateHeroEyebrow(snap); } catch (e) { console.warn('hero eyebrow render failed', e); }

    // Aggregate pill (top-right)
    const lvl = (snap.status && snap.status.level) || 'frozen';
    const sev = lvl === 'live' ? 'live' : (lvl === 'stale' ? 'warn' : 'err');
    const acledMax  = snap.acled  && snap.acled.newest_date;
    const brentMax  = snap.brent  && snap.brent.newest_date;
    let label;
    if (lvl === 'live') {
      label = `LIVE · MARKETS ${_frFmtDate(brentMax)} · EVENTS ${_frFmtDate(acledMax)}`;
    } else if (lvl === 'stale') {
      label = `STALE · EVENTS THROUGH ${_frFmtDate(acledMax)}`;
    } else {
      // FROZEN — usually means ACLED/Iran data is locked at the thesis window
      // even though Brent + transits are live. Be specific so the user knows
      // exactly which sources are current and which are not.
      const marketBit = brentMax ? `MARKETS ${_frFmtDate(brentMax)}` : 'MARKETS N/A';
      const eventBit  = acledMax ? `EVENTS ${_frFmtDate(acledMax)}`   : 'EVENTS N/A';
      label = `${marketBit} · ${eventBit}`;
    }
    setConnectionStatus(lvl !== 'frozen', label, sev);

    // Per-chokepoint "data through" caption — only update if the element
    // is present (rendered by hydrateChokepointCards).
    const transitMap = {
      hormuz: snap.hormuz_transits && snap.hormuz_transits.newest_month,
      bab:    snap.bab_transits    && snap.bab_transits.newest_month,
      suez:   snap.suez_transits   && snap.suez_transits.newest_month,
    };
    document.querySelectorAll('[data-cp-asof]').forEach(el => {
      const cp = el.getAttribute('data-cp-asof');
      const m  = transitMap[cp];
      el.textContent = m ? `Transit data through ${_frFmtMonth(m)} · IMF PortWatch` : '';
    });

    // §02.2 vt-foot freshness — make iframe panels honest about the AIS
    // source (3rd-party MarineTraffic free embed; shows all vessel
    // classes because MarineTraffic gates server-side filtering behind
    // a paid plan).
    document.querySelectorAll('[data-vt-asof]').forEach(el => {
      el.textContent = 'live AIS · all classes (free MT embed)';
    });
  }

  // ── Live monthly event counts (HDX ACLED mirror) ──────────────────────────
  // Backed by /api/live-event-counts which pulls the public ACLED→HDX feed.
  // HDX republishes ACLED weekly, so even when the live ACLED OAuth API is
  // unreachable from Render, this gives the dashboard a guaranteed-fresh
  // monthly events series. We map countries → chokepoints:
  //   Yemen + Saudi    → bab    (Houthi attacks + Saudi naval responses)
  //   Iran             → hormuz (IRGC, Iranian-coast operations)
  //   Egypt + Lebanon  → suez   (Sinai militancy, Hezbollah cross-fires)
  // Sums the most recent month across each chokepoint's contributing
  // countries and writes a "<month> · <count> events" caption to the
  // [data-cp-events-month] / [data-cp-events-count] elements.
  const FR_CP_COUNTRY_MAP = {
    bab:    ['yemen', 'saudi'],
    hormuz: ['iran'],
    suez:   ['egypt', 'lebanon'],
  };
  function hydrateLiveEventCounts(snap) {
    if (!snap || !snap.by_country) return;
    window.LIVE_EVENT_COUNTS = snap;

    const newestMonth = snap.newest_month || null;

    for (const [cp, countries] of Object.entries(FR_CP_COUNTRY_MAP)) {
      let totalEvents = 0;
      let totalFatal = 0;
      let monthsAvail = new Set();
      let cpNewest = '';
      for (const c of countries) {
        const rows = snap.by_country[c] || [];
        if (!rows.length) continue;
        // Use the newest month each country has — if they're not aligned,
        // we still pick the most recent point to keep the freshness pill honest.
        const last = rows[rows.length - 1];
        if (last.month > cpNewest) cpNewest = last.month;
        totalEvents += Number(last.events || 0);
        totalFatal  += Number(last.fatalities || 0);
        monthsAvail.add(last.month);
      }
      const monthEl = document.querySelector(`[data-cp-events-month="${cp}"]`);
      const countEl = document.querySelector(`[data-cp-events-count="${cp}"]`);
      const fatalEl = document.querySelector(`[data-cp-events-fatal="${cp}"]`);
      if (monthEl) monthEl.textContent = cpNewest ? _frFmtMonth(cpNewest) : '—';
      if (countEl) countEl.textContent = totalEvents > 0 ? totalEvents.toLocaleString() : '—';
      if (fatalEl) fatalEl.textContent = totalFatal > 0 ? totalFatal.toLocaleString() + '†' : '—';
    }

    // Top-of-section banner for §02 — single-line "live data through <month>"
    const banner = document.querySelector('[data-live-events-banner]');
    if (banner) {
      const mon = newestMonth ? _frFmtMonth(newestMonth) : '—';
      const ctry = Object.keys(snap.by_country || {}).length;
      const errors = Object.keys(snap.errors || {}).length;
      banner.textContent = errors
        ? `LIVE ACLED · HDX MIRROR · ${ctry} OF ${ctry + errors} COUNTRIES · THROUGH ${mon}`
        : `LIVE ACLED · HDX MIRROR · ${ctry} COUNTRIES · THROUGH ${mon}`;
    }

    // Bab body sentence — replaces the previously-static "100+ vessels
    // since Nov 2023" claim with the live HDX Yemen monthly event count.
    // Re-runs whenever LIVE_EVENT_COUNTS updates so the body sentence
    // stays current.
    const babLive = document.querySelector('[data-bab-live-events]');
    const yemenRows = (snap.by_country && snap.by_country.yemen) || [];
    if (babLive && yemenRows.length) {
      const last = yemenRows[yemenRows.length - 1];
      babLive.textContent = (last.events || 0).toLocaleString();
    }
  }

  // ── CHOKEPOINT STATUS FROM LIVE TRANSITS + EVENTS ─────────────────────────
  // Backend gives us month-level transit counts. We turn those into:
  //   - threat level (safe/elevated/high/critical)
  //   - threatPct  (0-100)
  //   - vesselsInZone (most recent month)
  //   - incidents30d (events within 200km of centroid in last 30 days)

  function classifyThreat(pctDecline) {
    // Higher decline = greater threat
    if (pctDecline == null) return { threat: 'elevated', pct: 50 };
    if (pctDecline >= 60)  return { threat: 'critical', pct: Math.min(100, 60 + pctDecline * 0.6) };
    if (pctDecline >= 30)  return { threat: 'high',     pct: 40 + pctDecline };
    if (pctDecline >= 10)  return { threat: 'elevated', pct: 20 + pctDecline * 1.5 };
    return                        { threat: 'safe',     pct: Math.max(5, 10 - pctDecline) };
  }

  function haversineKm(lat1, lon1, lat2, lon2) {
    const R = 6371;
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a = Math.sin(dLat / 2) ** 2 +
              Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
              Math.sin(dLon / 2) ** 2;
    return 2 * R * Math.asin(Math.sqrt(a));
  }

  // The ACLED fallback dataset is frozen at the thesis observation window
  // (ends early 2025). Anchoring "last 30 days" to wall-clock `Date.now()`
  // collapses the KPI to 0 once real time passes the dataset's end. Instead,
  // anchor to the most recent event date in the dataset so the window means
  // "last 30 days of observation". If the data is later refreshed past the
  // current date, this naturally tracks real time again.
  function dataNowTs(events) {
    if (!Array.isArray(events) || !events.length) return Date.now();
    let max = 0;
    for (const e of events) {
      const d = e.event_date || e.date;
      if (!d) continue;
      const t = Date.parse(d);
      if (!isNaN(t) && t > max) max = t;
    }
    // If the dataset extends past "now" (hypothetical) stay on real-time.
    return max ? Math.min(max, Date.now()) : Date.now();
  }

  function countRecentIncidents(events, cp, km = 300, days = 30) {
    // Wall-clock cutoff. Earlier versions used dataNowTs(events) (the
    // dataset's newest event) as the anchor — under ACLED's free-tier
    // 12-month embargo that anchored "30D" to a window ending in April
    // 2025, so the KPI counted year-old incidents as recent. The honest
    // count is "events in the last 30 calendar days from now"; the
    // /api/chokepoint-incidents pipeline already augments this with
    // curated war-timeline + news-promoted events so the count reflects
    // real recent oil-impactful activity even when raw ACLED is empty.
    if (!Array.isArray(events)) return 0;
    const now    = Date.now();
    const cutoff = now - days * 86400000;
    let n = 0;
    for (const e of events) {
      // Mirror the backend chokepoint_incidents filter: exclude
      // ACLED Protests / Riots from the kill-zone count. The hero
      // INCIDENTS · 30D card is supposed to count maritime-relevant
      // events, not peaceful demonstrations.
      const etype = (e.event_type || '').toLowerCase();
      if (etype === 'protests' || etype === 'riots') continue;
      const lat = +e.latitude, lon = +e.longitude;
      if (!lat || !lon) continue;
      const d = e.event_date || e.date;
      if (!d) continue;
      const t = Date.parse(d);
      if (isNaN(t) || t < cutoff || t > now) continue;
      if (haversineKm(lat, lon, cp.lat, cp.lon) <= km) n++;
    }
    return n;
  }

  // Expose for charts.js so its scrubber-aware KPI uses the same anchor.
  window.__dataNowTs = dataNowTs;

  // Per-chokepoint radius for the kill-zone count. Sourced from
  // window.CONSTANTS (set from /api/constants on bootstrap) so config.py
  // is the single source of truth. Local fallback included to keep the
  // page functional if the constants endpoint hasn't resolved yet.
  function RADIUS_KM_for(cp) {
    const c = (window.CONSTANTS && window.CONSTANTS.chokepoint_radius_km) || {};
    return c[cp] || ({ hormuz: 650, bab: 400, suez: 300 }[cp] || 300);
  }

  // ACLED's primary "events" payload is Yemen + regional coverage, which
  // EXCLUDES Iran. Hormuz-adjacent incidents (IRGC seizures, Iranian coastal
  // strikes, etc.) live in the separate /api/iran-events payload. Merge both
  // when scoring Hormuz so the KPI doesn't collapse to 0 just because the
  // action moved to the north side of the strait.
  function eventsForChokepoint(key, acledEvents, iranEvents) {
    const base = Array.isArray(acledEvents) ? acledEvents : [];
    if (key !== 'hormuz') return base;
    const iran = Array.isArray(iranEvents) ? iranEvents : [];
    if (!iran.length) return base;
    // Dedupe on event_id_cnty if present — the two feeds can overlap on
    // bilateral US/Iran incidents that also appear in regional pulls.
    const seen = new Set();
    const out = [];
    for (const e of base) {
      const id = e && e.event_id_cnty;
      if (id) seen.add(id);
      out.push(e);
    }
    for (const e of iran) {
      const id = e && e.event_id_cnty;
      if (id && seen.has(id)) continue;
      out.push(e);
    }
    return out;
  }

  function hydrateChokepoint(key, transitData, events, flowMbd, iranEvents) {
    const cp = window.CHOKEPOINTS[key];
    if (!cp) return;

    let vessels = null, decline = null;
    if (Array.isArray(transitData) && transitData.length >= 3) {
      const sorted = transitData.slice().sort((a, b) => a.month.localeCompare(b.month));
      // Baseline: pre Feb 2026 average
      const pre = sorted.filter(m => m.month < '2026-02');
      const cur = sorted.filter(m => m.month >= '2026-02');
      if (pre.length && cur.length) {
        const baseAvg = pre.reduce((s, m) => s + (m.transits || 0), 0) / pre.length;
        const curAvg  = cur.reduce((s, m) => s + (m.transits || 0), 0) / cur.length;
        if (baseAvg > 0) decline = ((1 - curAvg / baseAvg) * 100);
        vessels = Math.round(sorted[sorted.length - 1].transits || 0);
      } else {
        vessels = Math.round(sorted[sorted.length - 1].transits || 0);
      }
    }

    const { threat, pct } = classifyThreat(decline);
    cp.threat        = threat;
    cp.threatPct     = Math.round(pct);
    cp.flowMbd       = flowMbd;
    cp.vesselsInZone = vessels;
    const evForCount = eventsForChokepoint(key, events, iranEvents);
    cp.incidents30d  = countRecentIncidents(evForCount, cp, RADIUS_KM_for(key), 30);
    cp.pctDecline    = decline == null ? null : +decline.toFixed(1);
    cp.transitHistory = transitData || [];
  }

  // Cape route is an alternate — threat is inverse of Hormuz decline (diversions)
  function hydrateCape(hormuzTransits) {
    const cp = window.CHOKEPOINTS.cape;
    cp.threat    = 'safe';
    cp.threatPct = 8;
    cp.flowMbd   = 0;
    cp.incidents30d = 0;
    // Vessels diverted = proportional to Hormuz decline
    if (Array.isArray(hormuzTransits) && hormuzTransits.length >= 2) {
      const sorted = hormuzTransits.slice().sort((a, b) => a.month.localeCompare(b.month));
      const pre = sorted.filter(m => m.month < '2026-02');
      const cur = sorted.filter(m => m.month >= '2026-02');
      if (pre.length && cur.length) {
        const diverted = pre.reduce((s, m) => s + (m.transits || 0), 0) / pre.length
                       - cur.reduce((s, m) => s + (m.transits || 0), 0) / cur.length;
        cp.vesselsInZone = Math.max(0, Math.round(diverted));
      }
    }
  }

  // ── Threat-strip pills (header) — re-classify text + color from CHOKEPOINTS ──
  // The static markup says CRITICAL/HIGH/DEGRADED/NOMINAL but the real threat
  // is computed from transit decline. Sync the pill text + class so the strip
  // doesn't lie when the backend reclassifies.
  const PILL_CLASS = {
    critical: 'threat-pill threat-critical',
    high:     'threat-pill threat-high',
    elevated: 'threat-pill threat-elevated',
    safe:     'threat-pill threat-safe',
    flow:     'threat-pill threat-safe',
    pending:  'threat-pill',
  };
  const PILL_LABEL = {
    critical: 'CRITICAL', high: 'HIGH', elevated: 'DEGRADED',
    safe: 'NOMINAL', flow: 'NOMINAL', pending: '—',
  };
  function hydrateThreatPills() {
    const pills = document.querySelectorAll('.threat-pill[data-cp]');
    pills.forEach(el => {
      const key = el.getAttribute('data-cp');
      const cp = key && window.CHOKEPOINTS && window.CHOKEPOINTS[key];
      if (!cp) return;
      const t = cp.threat || 'pending';
      // Preserve original display name (everything before the · separator)
      const orig = el.textContent.split('·')[0].trim() || (cp.name || key.toUpperCase());
      el.className = PILL_CLASS[t] || PILL_CLASS.pending;
      el.textContent = `${orig} · ${PILL_LABEL[t] || t.toUpperCase()}`;
    });
  }

  // ── Build ATTACKS heatmap points for the globe ────────────────────────────
  function buildAttackHotspots(events) {
    if (!Array.isArray(events) || !events.length) return [];
    // Bucket into 1° grid, weight by count + fatalities
    const buckets = new Map();
    const cutoff = Date.now() - 180 * 86400000; // last 6 months
    for (const e of events) {
      const lat = +e.latitude, lon = +e.longitude;
      if (!lat || !lon) continue;
      const d = e.event_date || e.date;
      if (d && Date.parse(d) < cutoff) continue;
      const k = `${Math.round(lat)}|${Math.round(lon)}`;
      const b = buckets.get(k) || { lat: Math.round(lat), lon: Math.round(lon), n: 0, fat: 0 };
      b.n++;
      b.fat += (+e.fatalities || 0);
      buckets.set(k, b);
    }
    const arr = [...buckets.values()];
    if (!arr.length) return [];
    const maxN = Math.max(...arr.map(b => b.n));
    return arr
      .sort((a, b) => b.n - a.n)
      .slice(0, 60)
      .map(b => ({ lat: b.lat, lon: b.lon, intensity: Math.min(1, b.n / maxN), count: b.n }));
  }

  // ── Hero KPI row (Overview tab) ───────────────────────────────────────────
  function hydrateHero(master, brent) {
    const kpis = (master && master.kpis) || {};
    const latest = kpis.latest_brent_price ?? U.lastVal(brent, 'price');
    const prev   = (Array.isArray(brent) && brent.length >= 2) ? brent[brent.length - 2].price : null;
    const change24 = (latest != null && prev != null) ? (latest - prev) : (kpis.brent_price_change ?? null);
    const pct24    = (latest != null && prev != null && prev !== 0) ? ((latest - prev) / prev * 100) : null;

    setText('heroPrice',   latest != null ? latest.toFixed(2) : '—');
    const change = $('heroChange') || document.querySelector('.kpi-hero-change');
    if (change) {
      if (change24 == null) {
        change.textContent = '—';
      } else {
        const arrow = change24 > 0 ? '▲' : change24 < 0 ? '▼' : '•';
        const sign  = change24 > 0 ? '+' : '';
        change.innerHTML = `<span class="change-arrow">${arrow}</span> ${sign}$${change24.toFixed(2)}${pct24!=null?` (${sign}${pct24.toFixed(2)}%)`:''} · 24h`;
        change.classList.toggle('up', change24 > 0);
        change.classList.toggle('down', change24 < 0);
      }
    }

    // "Feb 28 → now: N days of market whiplash" — recompute daily so the
    // headline doesn't drift the moment the dashboard is left open overnight.
    const warOnset = new Date(((window.CONSTANTS && window.CONSTANTS.anchors && window.CONSTANTS.anchors.war_onset) || '2026-02-28') + 'T00:00:00Z');
    const daysSinceWar = Math.max(0, Math.floor((Date.now() - warOnset.getTime()) / 86400000));
    document.querySelectorAll('[data-days-since-war]').forEach(el => {
      el.textContent = `Feb 28 → now: ${daysSinceWar} days of market whiplash.`;
    });

    // 30-day range foot row + war-onset % (also surfaced in threat-strip below)
    let warPct = null;
    const foot = document.querySelector('.kpi-hero-foot');
    if (Array.isArray(brent) && brent.length) {
      const last30 = brent.slice(-30).map(r => r.price).filter(v => v != null);
      const _warDate = (window.CONSTANTS && window.CONSTANTS.anchors && window.CONSTANTS.anchors.war_onset) || '2026-02-28';
      const warStart = brent.find(r => r.date >= _warDate);
      warPct = (warStart && latest != null) ? ((latest - warStart.price) / warStart.price * 100) : null;
      if (foot && last30.length) {
        const lo = Math.min(...last30), hi = Math.max(...last30);
        foot.innerHTML = `
          <span>30D RANGE <b>$${lo.toFixed(2)} — $${hi.toFixed(2)}</b></span>
          <span>SINCE WAR ONSET <b class="${warPct>=0?'up':'down'}">${warPct!=null?(warPct>=0?'+':'')+warPct.toFixed(1)+'%':'—'}</b></span>`;
      }
    }

    // Header threat-bar KPIs
    setText('threatBrent', latest != null ? '$' + latest.toFixed(2) : '—');
    setText('threatOvx',   kpis.latest_ovx != null ? kpis.latest_ovx.toFixed(1) : '—');

    // Threat-strip Brent delta (24h $-change) and OVX delta (vs prior session)
    const brentDeltaEl = $('threatBrentDelta');
    if (brentDeltaEl) {
      if (pct24 == null) {
        brentDeltaEl.textContent = '—';
        brentDeltaEl.className = '';
      } else {
        const arrow = pct24 > 0 ? '▲' : pct24 < 0 ? '▼' : '•';
        const sign  = pct24 > 0 ? '+' : '';
        brentDeltaEl.textContent = `${arrow} ${sign}${pct24.toFixed(1)}%`;
        brentDeltaEl.className = pct24 >= 0 ? 'delta-up' : 'delta-down';
      }
    }
    const ovxDeltaEl = $('threatOvxDelta');
    if (ovxDeltaEl) {
      // Prior OVX from master.timeseries if present (we don't fetch /api/ovx separately here)
      const ts = (master && master.timeseries) || [];
      let prevOvx = null;
      for (let i = ts.length - 2; i >= 0; i--) {
        if (ts[i].ovx != null) { prevOvx = ts[i].ovx; break; }
      }
      const cur = kpis.latest_ovx;
      if (cur != null && prevOvx != null) {
        const d = cur - prevOvx;
        const arrow = d > 0 ? '▲' : d < 0 ? '▼' : '•';
        const sign = d > 0 ? '+' : '';
        ovxDeltaEl.textContent = `${arrow} ${sign}${d.toFixed(1)}`;
        ovxDeltaEl.className = d >= 0 ? 'delta-up' : 'delta-down';
      } else {
        ovxDeltaEl.textContent = '—';
        ovxDeltaEl.className = '';
      }
    }
    // WAR PREM = % vs Feb-28-2026 onset (defined above)
    const warPremEl = $('threatWarPrem');
    if (warPremEl) {
      warPremEl.textContent = warPct == null ? '—' : (warPct >= 0 ? '+' : '') + warPct.toFixed(1) + '%';
    }

    // Sparkline path
    const sparkLine = $('sparkLine'), sparkPath = $('sparkPath');
    if (sparkLine && Array.isArray(brent) && brent.length) {
      const data = brent.slice(-30).map(r => r.price).filter(v => v != null);
      if (data.length >= 2) {
        const w = 200, h = 50, pad = 2;
        const min = Math.min(...data), max = Math.max(...data);
        const range = max - min || 1;
        const x = i => pad + (i / (data.length - 1)) * (w - pad * 2);
        const y = v => h - pad - ((v - min) / range) * (h - pad * 2);
        const line = data.map((v, i) => `${i === 0 ? 'M' : 'L'}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(' ');
        const fill = line + ` L${x(data.length - 1).toFixed(1)},${h} L${x(0).toFixed(1)},${h} Z`;
        sparkLine.setAttribute('d', line);
        if (sparkPath) sparkPath.setAttribute('d', fill);
      }
    }
    // Expose for legacy code that looks at window.BRENT_SPARK
    if (Array.isArray(brent)) {
      window.BRENT_SPARK = brent.slice(-30).map(r => r.price);
    }
  }

  // ── Secondary KPIs — OVX, DXY, Suez throughput, flow-at-risk ──────────────
  function hydrateSecondaryKpis(master, suezTransits, hormuzTransits, babTransits) {
    const kpis = (master && master.kpis) || {};

    // OVX
    const ovxEl = document.querySelector('[data-kpi="ovx"]');
    if (ovxEl && kpis.latest_ovx != null) {
      const arrow = kpis.latest_ovx > 40 ? '<span class="up">▲</span>' : '<span class="down">▼</span>';
      ovxEl.innerHTML = `${kpis.latest_ovx.toFixed(1)} ${arrow}`;
    }

    // PEAK DAILY VOL — was static "8.7%" in markup; back it with the actual
    // max daily Brent return from the regression sample. data_service returns
    // peak_volatility as a percent (e.g. 7.266 = 7.3%); display one decimal.
    const peakVolEl = document.querySelector('[data-kpi="peakVol"]');
    if (peakVolEl && kpis.peak_volatility != null) {
      peakVolEl.textContent = `${(+kpis.peak_volatility).toFixed(1)}%`;
    }

    // DXY
    const dxyEl = document.querySelector('[data-kpi="dxy"]');
    if (dxyEl && kpis.latest_dxy != null) {
      const arrow = kpis.latest_dxy > 100 ? '<span class="up">▲</span>' : '<span class="down">▼</span>';
      dxyEl.innerHTML = `${kpis.latest_dxy.toFixed(1)} ${arrow}`;
    }

    // Suez throughput % change vs pre-war
    const suezEl = document.querySelector('[data-kpi="suez"]');
    if (suezEl && Array.isArray(suezTransits) && suezTransits.length >= 3) {
      const sorted = suezTransits.slice().sort((a, b) => a.month.localeCompare(b.month));
      const pre = sorted.filter(m => m.month < '2023-11');
      const cur = sorted.slice(-3);
      if (pre.length && cur.length) {
        const base = pre.reduce((s, m) => s + (m.transits || 0), 0) / pre.length;
        const now  = cur.reduce((s, m) => s + (m.transits || 0), 0) / cur.length;
        const pctChg = base > 0 ? ((now - base) / base * 100) : null;
        if (pctChg != null) {
          const sign = pctChg >= 0 ? '+' : '';
          const arrow = pctChg >= 0 ? '<span class="up">▲</span>' : '<span class="down">▼</span>';
          suezEl.innerHTML = `${sign}${pctChg.toFixed(0)}% ${arrow}`;
        }
      }
    }

    // Flow at risk: aggregate of Hormuz + Bab el-Mandeb in mbd (fixed geophysical estimate)
    const flowEl = document.querySelector('[data-kpi="flowAtRisk"]') ||
                   document.querySelector('.kpi-primary-warn .kpi-value');
    if (flowEl && window.CHOKEPOINTS.hormuz.flowMbd != null && window.CHOKEPOINTS.bab.flowMbd != null) {
      const total = window.CHOKEPOINTS.hormuz.flowMbd + window.CHOKEPOINTS.bab.flowMbd;
      flowEl.innerHTML = `${total.toFixed(1)} <span class="kpi-unit">mbd</span>`;
      const foot = flowEl.parentElement.querySelector('.kpi-foot');
      if (foot) foot.textContent = `Hormuz ${window.CHOKEPOINTS.hormuz.flowMbd.toFixed(1)} + Bab el-Mandeb ${window.CHOKEPOINTS.bab.flowMbd.toFixed(1)}`;
    }

    // Incidents KPI row — count of maritime events (excl. protests) within
    // 30 days of the dataset's newest event. Anchored to data not wall-clock.
    const incidentEl = document.querySelector('[data-kpi="incidents30"]');
    if (incidentEl) {
      const h = (window.CHOKEPOINTS.hormuz && window.CHOKEPOINTS.hormuz.incidents30d) || 0;
      const b = (window.CHOKEPOINTS.bab    && window.CHOKEPOINTS.bab.incidents30d)    || 0;
      incidentEl.textContent = String(h + b);
    }
    // Asof label — shows what 30-day window the count actually covers.
    // Without this, "INCIDENTS · 30D" reads as "last 30 days from today"
    // when in reality it's "last 30 days of available ACLED data" which
    // can be ~12 months behind today due to the free-tier embargo.
    const asofEl = document.querySelector('[data-kpi-foot-asof="incidents30"]');
    if (asofEl && Array.isArray(window.CP && window.CP.state && window.CP.state.events)) {
      const ts = window.__dataNowTs(window.CP.state.events);
      if (ts) {
        const d = new Date(ts);
        const months = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
        asofEl.textContent = `${String(d.getUTCDate()).padStart(2,'0')} ${months[d.getUTCMonth()]} ${d.getUTCFullYear()}`;
      }
    }
  }

  // ── Market Context KPIs — Tier-1 FRED/EIA ──────────────────────────────────
  // Populates the WTI-Brent spread, VIX, HY yield, and US crude inventory
  // tiles. All four sources are live APIs; silently skip any tile whose
  // endpoint returned empty (Render free-tier cold-start can leave one of
  // these unfetched on first paint — they'll hydrate on next cache cycle).
  function hydrateMarketContext(macro, inventories, brent) {
    const last = (arr) => (Array.isArray(arr) && arr.length) ? arr[arr.length - 1] : null;

    // WTI – Brent spread (signed, 2dp). Positive = WTI premium (domestic oversupply rare);
    // negative = Brent premium (tight seaborne supply, the common war-regime signal).
    const wtiBrentEl = document.querySelector('[data-kpi="wtiBrent"]');
    if (wtiBrentEl) {
      const wti = last(macro && macro.wti);
      // `brent` is the full Brent array (price field); find the most recent row that has a WTI date match,
      // else just take the last Brent price available.
      let brentLast = null;
      if (Array.isArray(brent) && brent.length) {
        if (wti && wti.date) {
          // Walk backward to find the Brent row on-or-before the WTI date (markets close slightly differently).
          for (let i = brent.length - 1; i >= 0; i--) {
            if (brent[i].date <= wti.date) { brentLast = brent[i]; break; }
          }
        }
        if (!brentLast) brentLast = brent[brent.length - 1];
      }
      if (wti && brentLast) {
        const spread = (+wti.value) - (+brentLast.price);
        const sign = spread >= 0 ? '+' : '';
        const arrow = spread >= 0 ? '<span class="up">▲</span>' : '<span class="down">▼</span>';
        wtiBrentEl.innerHTML = `${sign}$${spread.toFixed(2)} ${arrow}`;
      }
    }

    // VIX — S&P 500 volatility. >25 = elevated.
    const vixEl = document.querySelector('[data-kpi="vix"]');
    if (vixEl) {
      const v = last(macro && macro.vix);
      if (v && v.value != null) {
        const arrow = v.value > 20 ? '<span class="up">▲</span>' : '<span class="down">▼</span>';
        vixEl.innerHTML = `${(+v.value).toFixed(1)} ${arrow}`;
      }
    }

    // HY effective yield (%). Higher = credit-market stress.
    const hyEl = document.querySelector('[data-kpi="hyYield"]');
    if (hyEl) {
      const h = last(macro && macro.hy_yield);
      if (h && h.value != null) {
        const arrow = h.value > 7.5 ? '<span class="up">▲</span>' : '<span class="down">▼</span>';
        hyEl.innerHTML = `${(+h.value).toFixed(2)}% ${arrow}`;
      }
    }

    // US commercial crude stocks — latest weekly reading (thousand barrels).
    // Display in million-bbl and show W/W change arrow.
    const stocksEl = document.querySelector('[data-kpi="crudeStocks"]');
    if (stocksEl && inventories && Array.isArray(inventories.commercial_crude) && inventories.commercial_crude.length >= 2) {
      const arr = inventories.commercial_crude;
      const cur = arr[arr.length - 1];
      const prev = arr[arr.length - 2];
      if (cur && prev && cur.value != null && prev.value != null) {
        const delta = cur.value - prev.value;  // thousands of bbl
        const mmbbl = cur.value / 1000;        // convert to million bbl
        const arrow = delta >= 0 ? '<span class="up">▲</span>' : '<span class="down">▼</span>';
        const dSign = delta >= 0 ? '+' : '';
        stocksEl.innerHTML = `${mmbbl.toFixed(0)} mmbbl <span class="kpi-unit" style="font-size:10px;opacity:.7">${dSign}${(delta/1000).toFixed(1)} W/W</span> ${arrow}`;
      }
    }
  }

  // ── GDELT tone chart (Iran media sentiment) ───────────────────────────────
  // Small Chart.js line chart on the US-Iran tab. Renders only if the
  // canvas + data are both present; safe no-op otherwise.
  //
  // Tab-visibility caveat: the canvas lives inside #tab-iran which is
  // display:none on first paint. Chart.js measures the canvas's parent
  // at construction time — a hidden parent = 0×0 surface and the chart
  // stays invisible even after the tab is shown. Fix: cache the last
  // payload and re-render on the 'tab-changed' event when the user
  // navigates to the US-Iran tab.
  let __gdeltChartInstance = null;
  let __gdeltLastPayload = null;
  window.addEventListener('tab-changed', (ev) => {
    if (!ev || !ev.detail || ev.detail.name !== 'iran') return;
    if (!__gdeltLastPayload) return;
    // Wait one frame so the new tab's display:block is committed and the
    // canvas has real dimensions before Chart.js measures.
    requestAnimationFrame(() => {
      try { renderGdeltChart(__gdeltLastPayload); } catch (e) { console.warn('gdelt re-render failed', e); }
    });
  });
  // Exposed for the retry button in app.js — same closure, but app.js
  // can't reach it directly because hydrate.js is its own IIFE scope.
  window.__renderGdelt = (g) => renderGdeltChart(g);
  function _gdeltShowEmpty(canvas, msg) {
    // Replace the canvas's parent inner overlay with an honest empty
    // state rather than leaving a blank rectangle. The user explicitly
    // reported the chart "not loading"; silent no-op was the wrong UX.
    const parent = canvas.parentElement;
    if (!parent) return;
    let overlay = parent.querySelector('.gdelt-empty');
    if (!overlay) {
      overlay = document.createElement('div');
      overlay.className = 'gdelt-empty';
      overlay.style.cssText = 'position:absolute;inset:0;display:flex;align-items:center;justify-content:center;text-align:center;padding:24px;font-family:JetBrains Mono,monospace;font-size:11px;letter-spacing:1px;color:#8794a7;line-height:1.6;';
      if (getComputedStyle(parent).position === 'static') parent.style.position = 'relative';
      parent.appendChild(overlay);
    }
    overlay.innerHTML = msg;
  }
  function renderGdeltChart(gdelt) {
    const canvas = document.getElementById('gdeltToneChart');
    if (!canvas || !window.Chart) return;
    if (!gdelt) {
      _gdeltShowEmpty(canvas, 'GDELT request failed.<br><br><span data-gdelt-retry style="display:inline-block;cursor:pointer;padding:6px 14px;border:1px solid rgba(0,212,255,0.5);background:rgba(0,212,255,0.08);color:#3d99d4;border-radius:3px;font-weight:700;letter-spacing:1.4px">⟳ RETRY</span>');
      return;
    }
    __gdeltLastPayload = gdelt;
    const tone = Array.isArray(gdelt.tone) ? gdelt.tone : [];

    // Brent — also independently optional. We render whichever series is
    // present rather than bailing if either is empty. Fixes the case where
    // GDELT 429s TimelineTone but TimelineVolRaw + Brent are both fine —
    // previously the chart went blank waiting for tone; now Brent renders
    // and the empty-tone state is shown as a small overlay note.
    const brent = (window.CP && window.CP.state && Array.isArray(window.CP.state.brent))
      ? window.CP.state.brent : [];
    if (!tone.length && !brent.length) {
      _gdeltShowEmpty(canvas,
        'GDELT tone unavailable + Brent not yet loaded.<br>' +
        '<br><span data-gdelt-retry style="display:inline-block;cursor:pointer;padding:6px 14px;border:1px solid rgba(0,212,255,0.5);background:rgba(0,212,255,0.08);color:#3d99d4;border-radius:3px;font-weight:700;letter-spacing:1.4px">⟳ RETRY GDELT</span>');
      return;
    }
    // Clear any previous empty-state overlay before drawing.
    const overlay = canvas.parentElement && canvas.parentElement.querySelector('.gdelt-empty');
    if (overlay) overlay.remove();

    // Build the date axis from whichever source has dates. If both have
    // data we use tone's dates (the chart's primary signal). If only
    // Brent, we use Brent's dates filtered to the GDELT 90d window.
    let labels, values;
    if (tone.length) {
      const sorted = tone.slice().sort((a, b) => a.date.localeCompare(b.date));
      labels = sorted.map(r => r.date);
      values = sorted.map(r => +r.value);
    } else {
      // No tone — use last 90 days of Brent dates as the axis.
      const cutoff = new Date(Date.now() - 90 * 86400000).toISOString().slice(0, 10);
      const sorted = brent.filter(r => r.date >= cutoff).sort((a, b) => a.date.localeCompare(b.date));
      labels = sorted.map(r => r.date);
      values = [];  // no tone series to plot
    }

    // brent already captured above; just build the by-date lookup here.
    const brentByDate = new Map();
    for (const r of brent) brentByDate.set(r.date, +r.price);
    // Align Brent to the tone date axis. Markets close weekends/holidays
    // so Brent is sparse on those days; carry the prior trading-day's
    // close forward so the line is continuous and visually parseable.
    let lastClose = null;
    const brentSeries = labels.map(d => {
      if (brentByDate.has(d)) {
        lastClose = brentByDate.get(d);
        return lastClose;
      }
      return lastClose;  // forward-fill for non-trading days
    });
    const brentHasData = brentSeries.some(v => v != null);

    if (__gdeltChartInstance) {
      try { __gdeltChartInstance.destroy(); } catch (_) {}
    }
    // Build datasets conditionally — render whichever series is available.
    const datasets = [];
    if (tone.length) {
      datasets.push({
        label: 'Daily tone',
        data: values,
        borderColor: 'rgba(61, 153, 212, 0.55)',
        backgroundColor: 'rgba(61, 153, 212, 0.10)',
        borderWidth: 1.5,
        pointRadius: 0,
        fill: true,
        tension: 0.2,
        yAxisID: 'yTone',
      });
    }
    if (brentHasData) {
      datasets.push({
        label: 'Brent close ($/bbl)',
        data: brentSeries,
        borderColor: '#ffaa00',
        borderWidth: 2,
        pointRadius: 0,
        fill: false,
        tension: 0.2,
        spanGaps: true,
        yAxisID: 'yBrent',
      });
    }
    __gdeltChartInstance = new Chart(canvas.getContext('2d'), {
      type: 'line',
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: { labels: { color: '#c8d0dc', font: { family: 'JetBrains Mono', size: 10 } } },
          tooltip: {
            callbacks: {
              label: (ctx) => {
                const v = +ctx.parsed.y;
                if (ctx.dataset.label && ctx.dataset.label.startsWith('Brent')) {
                  return `${ctx.dataset.label}: $${v.toFixed(2)}`;
                }
                return `${ctx.dataset.label}: ${v.toFixed(2)}`;
              },
            },
          },
        },
        scales: {
          x: {
            ticks: { color: '#7e8699', font: { family: 'JetBrains Mono', size: 9 }, maxTicksLimit: 8 },
            grid: { color: 'rgba(255,255,255,0.04)' },
          },
          yTone: {
            type: 'linear',
            position: 'left',
            display: tone.length > 0,
            ticks: { color: 'rgba(61, 153, 212, 0.85)', font: { family: 'JetBrains Mono', size: 9 } },
            grid: { color: 'rgba(255,255,255,0.05)' },
            title: { display: true, text: 'GDELT TONE', color: 'rgba(61, 153, 212, 0.85)', font: { size: 10, weight: 'bold' } },
          },
          yBrent: {
            type: 'linear',
            position: tone.length ? 'right' : 'left',
            display: brentHasData,
            ticks: { color: '#ffaa00', font: { family: 'JetBrains Mono', size: 9 }, callback: v => '$' + v },
            grid: { display: !tone.length, color: 'rgba(255,255,255,0.05)' },
            title: { display: true, text: 'BRENT $/bbl', color: '#ffaa00', font: { size: 10, weight: 'bold' } },
          },
        },
      },
    });
    // If tone is missing, surface a small note over the chart explaining
    // why — the chart isn't broken, GDELT just hasn't returned tone yet.
    if (!tone.length && brentHasData) {
      const parent = canvas.parentElement;
      if (parent) {
        let note = parent.querySelector('.gdelt-tone-note');
        if (!note) {
          note = document.createElement('div');
          note.className = 'gdelt-tone-note';
          note.style.cssText = 'position:absolute;top:10px;left:14px;padding:5px 10px;background:rgba(255,170,0,0.10);border:1px solid rgba(255,170,0,0.40);color:#ffaa00;font-family:JetBrains Mono,monospace;font-size:10px;letter-spacing:1.2px;border-radius:3px;cursor:pointer;z-index:5;';
          note.setAttribute('data-gdelt-retry', '1');
          if (getComputedStyle(parent).position === 'static') parent.style.position = 'relative';
          parent.appendChild(note);
        }
        note.textContent = '⚠ GDELT TONE FETCHING — CLICK TO RETRY';
      }
    } else {
      const note = canvas.parentElement && canvas.parentElement.querySelector('.gdelt-tone-note');
      if (note) note.remove();
    }
  }

  // ── News & event feed ─────────────────────────────────────────────────────
  // Merge live Google News + curated war-period events into a single
  // sorted feed. Either source can be empty (Google News rate-limits
  // routinely, the backend's curated list is always present), so the
  // panel never goes blank as long as ONE of them has data.
  // De-dupes by title prefix so a curated event won't duplicate a live
  // headline that covers the same incident.
  function _mergeFeedItems(iranResp) {
    const news    = (iranResp && Array.isArray(iranResp.news))    ? iranResp.news    : [];
    const curated = (iranResp && Array.isArray(iranResp.curated)) ? iranResp.curated : [];
    const seen = new Set();
    const out = [];
    // Live news first (most recent real-world reporting); curated backfills.
    for (const arr of [news, curated]) {
      for (const item of arr) {
        const key = (item && item.title || '').toLowerCase().slice(0, 60);
        if (!key || seen.has(key)) continue;
        seen.add(key);
        out.push(item);
      }
    }
    // Sort descending by date so the freshest item is always first,
    // regardless of which source it came from.
    out.sort((a, b) => {
      const da = (a.published_at || a.date || '').slice(0, 10);
      const db = (b.published_at || b.date || '').slice(0, 10);
      return db.localeCompare(da);
    });
    return out;
  }
  // Exposed for the delayed-retry path so it doesn't have to inline the merge.
  window.__mergeFeedItems = _mergeFeedItems;


  function hydrateFeed(news) {
    const feed = $('feedList');
    if (!feed) return;
    // Cache the unfiltered list so the temporal scrubber can re-render
    // without needing a refetch.
    if (Array.isArray(news)) window.__feedRaw = news;
    const raw = Array.isArray(news) ? news : (window.__feedRaw || []);
    // Apply temporal scrubber: drop items dated after the scrubbed date.
    // Read date from either `published_at` (legacy news) or `date`
    // (Google News + curated events both use this field).
    const tl = window.CP && window.CP.timeline;
    const cutoffMs = (tl && !tl.atNow) ? tl.ts : null;
    const tlFiltered = cutoffMs
      ? raw.filter(n => {
          const ds = n.published_at || n.date;
          if (!ds) return true;
          const t = Date.parse(ds);
          return isNaN(t) || t <= cutoffMs;
        })
      : raw;
    // Apply cross-filter: keep items whose category fuzzy-matches the active
    // ACLED filter. If that wipes out the list (no overlap in taxonomies),
    // fall back to the time-filtered set so the panel never goes blank.
    const xf = (window.CP && window.CP.filters && window.CP.filters.eventType) || null;
    let filtered = tlFiltered;
    let xfBanner = null;
    if (xf && typeof window.xfMatchesNewsCat === 'function') {
      const m = tlFiltered.filter(n => window.xfMatchesNewsCat(xf, (n.category || n.type || '')));
      if (m.length) {
        filtered = m;
        xfBanner = `FILTER: ${xf.toUpperCase()}`;
      } else {
        xfBanner = `FILTER: ${xf.toUpperCase()} · NO MATCH`;
      }
    }
    const items = filtered.slice(0, 10);

    // Floating filter pill above the feed
    const wrap = feed.parentElement;
    if (wrap) {
      if (getComputedStyle(wrap).position === 'static') wrap.style.position = 'relative';
      let pill = wrap.querySelector('.xf-banner-feed');
      if (!xfBanner && pill) pill.remove();
      if (xfBanner) {
        if (!pill) {
          pill = document.createElement('div');
          pill.className = 'xf-banner-feed';
          pill.style.cssText = 'position:absolute;top:6px;right:8px;padding:3px 7px;font:9px "JetBrains Mono",monospace;letter-spacing:1.5px;border-radius:3px;background:rgba(0,212,255,0.15);color:#dfe7f0;border:1px solid rgba(0,212,255,0.45);z-index:5;pointer-events:none';
          wrap.appendChild(pill);
        }
        pill.textContent = xfBanner;
      }
    }

    if (!items.length) {
      feed.innerHTML = '<div class="feed-empty">No news in this window.</div>';
      window.FEED = [];
      return;
    }
    feed.innerHTML = items.map(n => {
      // Time/date display: prefer published_at (with HH:MM if available),
      // fall back to date field (DD MMM). Both news and curated events
      // have at least a date; only news has wall-clock time.
      const ds = n.published_at || n.date || '';
      const d = ds ? new Date(ds) : null;
      let when = '—';
      if (d && !isNaN(d)) {
        if (n.published_at) {
          when = `${String(d.getUTCHours()).padStart(2,'0')}:${String(d.getUTCMinutes()).padStart(2,'0')}Z`;
        } else {
          const months = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
          when = `${String(d.getUTCDate()).padStart(2,'0')} ${months[d.getUTCMonth()]}`;
        }
      }
      const type = (n.category || n.type || 'diplomatic').toLowerCase();
      // Source: live news has Google News source; curated events show
      // "CURATED" so the user knows it's a hand-picked timeline entry.
      const src = n.source || (n.auto === false || n.severity != null ? 'CURATED' : '');
      return `
        <div class="feed-item">
          <span class="feed-time">${when}</span>
          <span class="feed-type ft-${type}">${type.toUpperCase()}</span>
          <span class="feed-title">${(n.title || '').replace(/</g, '&lt;')}</span>
          <span class="feed-source">${String(src).replace(/</g, '&lt;')}</span>
        </div>`;
    }).join('');
    window.FEED = items;
  }
  // Re-render the feed when the temporal scrubber moves OR the cross-filter
  // toggles. Both events should re-run hydrateFeed against the cached raw
  // payload so we never re-fetch.
  if (typeof window !== 'undefined' && !window.__feedTlWired) {
    window.__feedTlWired = true;
    window.addEventListener('timeline-set', () => {
      if (window.__feedRaw) hydrateFeed(window.__feedRaw);
    });
    window.addEventListener('cross-filter-changed', () => {
      if (window.__feedRaw) hydrateFeed(window.__feedRaw);
    });
  }

  // ── Iran curated events timeline (for chart + event markers) ──────────────
  function hydrateIranEvents(iranResp) {
    const curated = (iranResp && Array.isArray(iranResp.curated)) ? iranResp.curated : [];
    const news    = (iranResp && Array.isArray(iranResp.news))    ? iranResp.news    : [];

    // Normalize curated entries first.
    //
    // The backend's get_merged_curated_events() returns a single list mixing
    // two flavors:
    //   • hand-curated war-period events (source_type:'curated') — the
    //     editorial spine of the chart, but the list ends at whatever date
    //     the dev last edited (currently ~Apr 17, 2026).
    //   • news-promoted entries (source_type:'news_auto') — top-scored
    //     Google News headlines that auto-extend the timeline past the
    //     curated cutoff. These are the "hollow rings" the chart subtitle
    //     advertises; without distinguishing them, they'd render as small
    //     solid dots indistinguishable from curated.
    //
    // Earlier code read `e.auto` here but that field doesn't exist — the
    // backend uses `source_type`. As a result curatedCutoff was computed
    // including news_auto dates → equaled today → liveEvents below
    // filtered to date > today → empty hollow-ring set, AND the
    // news_auto entries that did get plotted looked like ordinary
    // curated dots. Fix: read `source_type` correctly so news_auto is
    // tagged as `live-news` source.
    const curatedEvents = curated.map(e => ({
      date:  e.date,
      type:  (e.type || e.category || 'diplomatic').toLowerCase(),
      label: e.label || e.title || e.name || '',
      price: e.brent_price != null ? +e.brent_price : null,
      source: e.source_type === 'news_auto' ? 'live-news' : 'curated',
    })).filter(e => e.date);

    // Curated cutoff = the last HAND-CURATED event date (source_type:'curated').
    // Skip news_auto entries when computing the cutoff so today's news
    // doesn't push the cutoff to today and starve liveEvents below.
    let curatedCutoff = '';
    for (const e of curatedEvents) {
      if (e.source === 'live-news') continue;
      if (e.date > curatedCutoff) curatedCutoff = e.date;
    }

    // Live-news markers fill the post-curated gap. Without this, the §09
    // chart's price line continues live but event dots stop dead at the
    // curated list's last date — making the chart look "broken" even
    // though it's accurate. We promote only news items that look like
    // discrete events (military / diplomatic / sanctions / nuclear / proxy)
    // and that are dated AFTER the curated cutoff so we don't double-mark
    // events that already have a curated entry.
    //
    // Headline filter: lower-case keyword match on a small whitelist of
    // war-relevant verbs ("strike", "attack", "missile", "tanker",
    // "ceasefire", etc.) to avoid every politics article ending up on
    // the chart. Cap to 30 newest so we don't crater the chart's
    // legibility with dot soup.
    const LIVE_KEYWORDS = [
      'strike', 'strikes', 'struck', 'attack', 'attacks', 'attacked',
      'missile', 'missiles', 'drone', 'drones', 'shot down', 'shoots down',
      'tanker', 'tankers', 'hormuz', 'oil', 'crude', 'shipping',
      'killed', 'casualties', 'troops', 'naval', 'navy', 'irgc',
      'sanction', 'sanctions', 'sanctioned',
      'nuclear', 'enrichment', 'iaea', 'uranium',
      'ceasefire', 'truce', 'talks', 'negotiat', 'deal',
      'seize', 'seized', 'sink', 'sinks', 'sunk', 'mine', 'mined', 'mines',
      'blockade', 'closes', 'closed', 'closure', 'reopens', 'reopened',
    ];
    function _looksLikeEvent(title) {
      if (!title) return false;
      const t = title.toLowerCase();
      return LIVE_KEYWORDS.some(kw => t.includes(kw));
    }

    // Dedupe key: backend's news-promoted entries are already in curatedEvents
    // with source='live-news'. Skip any raw-news item whose title prefix already
    // matches one of those so we don't double-mark the same headline.
    const seenLive = new Set();
    for (const e of curatedEvents) {
      if (e.source === 'live-news' && e.label) {
        seenLive.add((e.label || '').slice(0, 60).toLowerCase());
      }
    }
    const liveEvents = news
      .filter(n => n && n.date && (!curatedCutoff || n.date > curatedCutoff))
      .filter(n => _looksLikeEvent(n.title))
      .filter(n => !seenLive.has((n.title || '').slice(0, 60).toLowerCase()))
      .map(n => ({
        date:   n.date,
        type:   (n.type || n.category || 'military').toLowerCase(),
        label:  n.title || '',
        price:  null,
        source: 'live-news',
        url:    n.url || null,
      }))
      .sort((a, b) => b.date.localeCompare(a.date))   // newest first
      .slice(0, 30);                                   // cap to 30 markers

    window.IRAN_EVENTS = [...curatedEvents, ...liveEvents]
      .sort((a, b) => a.date.localeCompare(b.date));
  }

  // ── Tactical-side stats (Geospatial tab) — count events by region.
  // Bounds match the regions visible on the tactical map.
  function inBox(lat, lon, latLo, latHi, lonLo, lonHi) {
    return lat >= latLo && lat <= latHi && lon >= lonLo && lon <= lonHi;
  }
  // Houthi-attributed events only. The ACLED dataset includes everything that
  // happens in Yemen — civil war crossfire, AQAP attacks, Iran-backed proxy
  // operations elsewhere, etc. The tactical map is specifically about the
  // Houthi maritime campaign so we filter on actor1/actor2 names. Coverage
  // includes the formal alias ("Ansar Allah") and common spelling variants.
  function isHouthiAttributed(e) {
    const a1 = (e.actor1 || '').toLowerCase();
    const a2 = (e.actor2 || '').toLowerCase();
    const blob = a1 + ' ' + a2;
    return blob.includes('houthi') || blob.includes('ansar allah') || blob.includes('ansarallah');
  }
  function hydrateTacStats(events) {
    if (!Array.isArray(events)) return;
    let total = 0, sRedSea = 0, gulfAden = 0, choke = 0;
    for (const e of events) {
      if (!isHouthiAttributed(e)) continue;
      const lat = +e.latitude, lon = +e.longitude;
      if (!lat || !lon) continue;
      // Red Sea + Gulf of Aden viewport
      if (!inBox(lat, lon, 5, 30, 32, 56)) continue;
      total++;
      if (inBox(lat, lon, 12, 18, 38, 44))   sRedSea++;
      if (inBox(lat, lon, 10.5, 14.5, 44, 52)) gulfAden++;
      if (inBox(lat, lon, 12.0, 13.3, 42.8, 43.8)) choke++;
    }
    setText('tacTotal',   total);
    setText('tacSouthRS', sRedSea);
    setText('tacGoA',     gulfAden);
    setText('tacChoke',   choke);
    // Also patch the badge near the section header so "N=726" stops lying
    const badge = document.querySelector('.narrative-block .badge.badge-gold');
    if (badge && /ACLED · N=/.test(badge.textContent)) {
      badge.textContent = `ACLED · N=${total}`;
    }
  }

  // ── Chokepoint cards (overview tab) — hydrate every stat from REAL data
  // Each value is sourced from a backend computation, not a UI scaling
  // hack. The previous "premium" proxy (threatPct/100*3.5%) was a made-up
  // multiplier with no relationship to actual oil/insurance premia and
  // has been removed in favor of the live transit-decline metric, which
  // is computed from real PortWatch transit data.
  // Exposed so app.js's refreshSidebarIncidents can re-render the chokepoint
  // cards after it promotes the merged-pool 30-day count up to
  // window.CHOKEPOINTS. Without this the cards stick to whatever the prior
  // hydrate pass wrote.
  window.__hydrateChokepointCards = () => hydrateChokepointCards();
  function hydrateChokepointCards() {
    const cards = document.querySelectorAll('[data-cp-card]');
    cards.forEach(card => {
      const key = card.getAttribute('data-cp-card');
      const cp = key && window.CHOKEPOINTS && window.CHOKEPOINTS[key];
      if (!cp) return;
      const set = (stat, val) => {
        const el = card.querySelector(`[data-stat="${stat}"]`);
        if (el != null && val != null) el.textContent = val;
      };
      // Risk pill
      if (cp.threat) set('risk', PILL_LABEL[cp.threat] || cp.threat.toUpperCase());
      // Flow (mbd) — only set if card has a flow stat hook (Hormuz/Bab; Cape uses nm)
      if (cp.flowMbd != null) set('flow', cp.flowMbd.toFixed(1));
      // Monthly transits — from PortWatch, comma-formatted
      if (cp.vesselsInZone != null) set('vessels', cp.vesselsInZone.toLocaleString());
      // Transit decline % vs. pre-Feb-2026 baseline (real, computed from
      // PortWatch). Replaces the fake "PREMIUM" stat. Sign-aware so a
      // recovery shows green +N%, a collapse shows red -N%.
      if (cp.pctDecline != null) {
        // pctDecline is positive when transits ARE down vs baseline.
        // Display as "−42%" (drop) or "+5%" (recovery above baseline).
        const v = -cp.pctDecline;  // flip sign so positive = improving
        const sign = v > 0 ? '+' : '';
        set('declinePct', `${sign}${v.toFixed(0)}%`);
      } else {
        set('declinePct', '—');
      }
      // Incidents/30d — from ACLED (thesis-window data; protests excluded
      // server-side). Number is real but anchored to the dataset's newest
      // event, not wall-clock — that's what the "(THESIS WINDOW)" sub-
      // label on the card communicates.
      if (cp.incidents30d != null) set('incidents', cp.incidents30d);
      // Cape: vessels diverted estimate. cp.vesselsInZone for cape was
      // already computed in hydrateCape() as a proportional estimate
      // from the Hormuz transit decline — labeled "EST. REROUTED" in
      // the markup so the user knows it's an estimate.
      if (key === 'cape' && cp.vesselsInZone != null) {
        set('diverted', cp.vesselsInZone.toLocaleString());
      }
    });
  }

  // ── Hypothesis cards (Analysis tab) — backed by /api/hypothesis ──
  // Critical: the previous static markup carried fabricated coefficients with
  // SUPPORTED verdicts. The thesis regression actually returns *negative*
  // coefficients (market adaptation), so verdicts are NOT SUPPORTED. Render
  // the live values verbatim with no opinion.
  function hydrateHypothesis(resp) {
    if (!resp) return;
    const fmt = (v, d = 3) => (v == null || isNaN(v)) ? '—' : Number(v).toFixed(d);
    ['h1', 'h2', 'h3'].forEach(key => {
      const card = document.querySelector(`.hyp-card[data-hyp="${key}"]`);
      if (!card) return;
      const h = resp[key];
      if (!h) return;
      const setSel = (sel, txt) => { const el = card.querySelector(sel); if (el) el.textContent = txt; };
      // Title — strip leading "H1: " / "H2: " etc. since the tag chip already shows it
      const cleanName = (h.name || '').replace(/^H\d:\s*/i, '') || '—';
      setSel('[data-hyp-name]', cleanName);
      setSel('[data-hyp-sub]', h.description || '—');
      setSel('[data-hyp-m="beta"]', fmt(h.coefficient, 3));
      setSel('[data-hyp-m="p"]',    fmt(h.p_value, 3));
      setSel('[data-hyp-m="r2"]',   fmt(h.r_squared, 3));

      // Verdict — derive from `supported` flag. Backend uses False for both
      // "not significant" and "significant but wrong sign"; distinguish so the
      // card communicates the actual finding (e.g. H1/H2 are sig. opposite).
      const verdict = card.querySelector('[data-hyp-verdict]');
      if (verdict) {
        verdict.classList.remove('hyp-supported', 'hyp-weak', 'hyp-not', 'hyp-not-sig');
        const beta = +h.coefficient;
        const p    = +h.p_value;
        if (h.supported === true) {
          verdict.textContent = 'SUPPORTED';
          verdict.classList.add('hyp-supported');
        } else if (h.supported === false) {
          if (!isNaN(p) && p < 0.05 && !isNaN(beta) && beta < 0) {
            verdict.textContent = 'NOT SUPPORTED · sig. opposite';
            verdict.classList.add('hyp-not');
          } else if (!isNaN(p) && p >= 0.05) {
            verdict.textContent = 'NOT SUPPORTED · n.s.';
            verdict.classList.add('hyp-not-sig');
          } else {
            verdict.textContent = 'NOT SUPPORTED';
            verdict.classList.add('hyp-not');
          }
        } else {
          verdict.textContent = '—';
        }
      }
      setSel('[data-hyp-plain]', h.conclusion || '—');
    });
  }

  // ── GARCH summary (Analysis tab §06) — fit stats from /api/hypothesis ──
  // Only fields the backend actually returns are surfaced. Fabricated ω/α/β/γ
  // parameter cells were removed from the markup.
  function hydrateGarch(resp) {
    const g = resp && resp.garch_summary;
    if (!g) return;
    const fmtNum = (v, d) => (v == null || isNaN(v)) ? '—' : Number(v).toFixed(d);
    document.querySelectorAll('[data-garch]').forEach(el => {
      const k = el.getAttribute('data-garch');
      const v = g[k];
      if (v == null || v === '') { el.textContent = '—'; return; }
      if (typeof v === 'number') {
        if (k === 'log_likelihood') el.textContent = fmtNum(v, 2);
        else if (k === 'aic' || k === 'bic') el.textContent = fmtNum(v, 1);
        else el.textContent = String(v);
      } else {
        el.textContent = String(v);
      }
    });
    // Section badge — show the actual model name instead of a fixed "ASYMMETRIC"
    const badge = document.querySelector('[data-garch-badge]');
    if (badge && g.model) badge.textContent = String(g.model).toUpperCase();
  }

  // ── Hero "X mbd of oil at risk" / "Y of global supply" claim ──
  // Sums Hormuz + Bab flows from CHOKEPOINTS (already populated above) and
  // expresses the share of ~100 mbd global liquids consumption (EIA).
  function hydrateHeroFlowClaim() {
    const h = window.CHOKEPOINTS && window.CHOKEPOINTS.hormuz;
    const b = window.CHOKEPOINTS && window.CHOKEPOINTS.bab;
    if (!h || !b || h.flowMbd == null || b.flowMbd == null) return;
    const total = h.flowMbd + b.flowMbd;
    // EIA STEO 2026 forecast for world liquids consumption (mbd). Sourced
    // from /api/constants → window.CONSTANTS so updating the macro number
    // happens in one place (config.py).
    const denom = (window.CONSTANTS && window.CONSTANTS.global_liquids_mbd) || 102.0;
    const pct = (total / denom) * 100;
    document.querySelectorAll('[data-hero-flow]').forEach(el => {
      el.textContent = total.toFixed(1);
    });
    document.querySelectorAll('[data-hero-flow-pct]').forEach(el => {
      el.textContent = `~${pct.toFixed(0)}%`;
    });
  }

  // ── Chokepoint card body sentence — transit decline vs pre-war ──
  // Replaces the previously-hardcoded "Tankers running at max speed since
  // Feb 28" claim with the live computed pctDecline.
  function hydrateChokepointDecline() {
    document.querySelectorAll('[data-cp-decline]').forEach(el => {
      const key = el.getAttribute('data-cp-decline');
      const cp = window.CHOKEPOINTS && window.CHOKEPOINTS[key];
      if (!cp) { el.textContent = '—'; return; }
      const d = cp.pctDecline;
      if (d == null || isNaN(d)) { el.textContent = '—'; return; }
      const sign = d >= 0 ? '−' : '+';        // Decline is positive ⇒ display as "−42%"
      el.textContent = `${sign}${Math.abs(d).toFixed(0)}% vs. pre-Feb 2026`;
    });
    // Bab body sentence — live HDX Yemen events count for the most recent
    // available month. Replaces the static "100+ vessels since Nov 2023"
    // claim that aged badly because the war has expanded since then.
    const babLive = document.querySelector('[data-bab-live-events]');
    if (babLive) {
      const live = window.LIVE_EVENT_COUNTS;
      const yemenRows = (live && live.by_country && live.by_country.yemen) || [];
      if (yemenRows.length) {
        const last = yemenRows[yemenRows.length - 1];
        babLive.textContent = (last.events || 0).toLocaleString();
      }
    }
  }

  // ── Hero eyebrow — replace "UPDATED HOURLY" with an honest breakdown
  // of which feeds are live and how recent each is. Pulled from the
  // freshness snapshot so it stays accurate as data ages.
  function hydrateHeroEyebrow(snap) {
    const el = document.querySelector('[data-hero-eyebrow-live]');
    if (!el || !snap) return;
    const lvl = (snap.status && snap.status.level) || 'frozen';
    const brentDate = snap.brent && snap.brent.newest_date;
    const fmt = (d) => {
      if (!d) return '';
      const months = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
      const [y, m, dd] = d.split('-').map(Number);
      return `${String(dd||1).padStart(2,'0')} ${months[(m||1)-1]} ${y}`;
    };
    const dot = lvl === 'live' ? '🟢' : lvl === 'stale' ? '🟡' : '🟠';
    const tail = brentDate ? `MARKETS LIVE THROUGH ${fmt(brentDate)}` : 'LIVE FEEDS';
    el.textContent = `${dot} ${tail}`;
  }

  // ── Hero body sentence — adapts the lead paragraph to the live threat
  // data so the dashboard's headline narrative tracks reality. Three
  // tiers based on Hormuz threat: NOMINAL/SAFE → "easing"; HIGH/ELEVATED
  // → "pressure"; CRITICAL → "near-closure". Falls back to the original
  // text if the threat data hasn't arrived yet.
  function hydrateHeroBody() {
    const el = document.querySelector('[data-hero-body]');
    if (!el) return;
    const h = window.CHOKEPOINTS && window.CHOKEPOINTS.hormuz;
    const b = window.CHOKEPOINTS && window.CHOKEPOINTS.bab;
    if (!h || !h.threat || !b) return;
    const lines = {
      critical: `<b>Hormuz transits have collapsed</b> (${h.pctDecline != null ? '−' + Math.abs(h.pctDecline).toFixed(0) + '% vs. pre-war' : 'severe decline'}) as the US–Iran war combines with continued Houthi pressure on Bab el-Mandeb. This is the most severe dual-chokepoint crisis since the 1980s Tanker War; the dashboard quantifies the market response.`,
      high:     `<b>Hormuz under sustained pressure</b> (${h.pctDecline != null ? '−' + Math.abs(h.pctDecline).toFixed(0) + '% vs. pre-war transits' : 'transits declining'}) while Houthi maritime operations continue to threaten Bab el-Mandeb. This dashboard tracks the dual-chokepoint risk premium in real time.`,
      elevated: `<b>Hormuz traffic degraded but flowing</b>; Houthi maritime operations continue to apply pressure on Bab el-Mandeb. The dashboard tracks how oil markets are pricing the dual-chokepoint risk.`,
      safe:     `<b>Conditions easing across both chokepoints.</b> The dashboard continues to monitor traffic, incidents, and market signals for any re-escalation.`,
      pending:  null,
    };
    const txt = lines[h.threat];
    if (txt) el.innerHTML = txt;
  }

  // ── Scenario cards — recompute target ranges against live Brent ──
  // The data-sc-mult-low / data-sc-mult-high attributes are analyst-set
  // multipliers (e.g. ESCALATION 1.49–1.70x). Multiplying by current spot
  // keeps the displayed dollar range honest as Brent moves.
  function hydrateScenarios(brent) {
    const last = (Array.isArray(brent) && brent.length) ? brent[brent.length - 1] : null;
    const spot = last && last.price != null ? +last.price : null;
    document.querySelectorAll('[data-scenario-base]').forEach(el => {
      el.textContent = spot != null ? spot.toFixed(2) : '—';
    });
    if (spot == null) return;
    document.querySelectorAll('.sc-target[data-sc-mult-low]').forEach(el => {
      const lo = +el.getAttribute('data-sc-mult-low');
      const hi = +el.getAttribute('data-sc-mult-high');
      if (isNaN(lo) || isNaN(hi)) return;
      el.textContent = `$${(spot * lo).toFixed(0)}–${(spot * hi).toFixed(0)}`;
    });
  }

  // ── US-Iran tab KPIs (4 cards) — backed by /api/iran-impact ──
  function hydrateIranKpis(resp) {
    const k = (resp && resp.kpis) || {};
    const set = (name, txt) => {
      const el = document.querySelector(`[data-iran-kpi="${name}"]`);
      if (el) el.textContent = txt;
    };
    set('total',     k.total_events != null ? k.total_events : '—');
    set('avg3d',     k.avg_price_move_3d != null ? '$' + k.avg_price_move_3d.toFixed(2) : '—');
    set('peak3d',    k.peak_volatility_spike != null ? '$' + k.peak_volatility_spike.toFixed(2) : '—');
    set('thisMonth', k.events_in_latest_month != null ? k.events_in_latest_month
                     : (k.events_this_month != null ? k.events_this_month : '—'));
    // Re-label the KPI card with the actual month so users don't read
    // "0 events this month" as broken — it's the most recent month
    // *available* in the data, which trails wall-clock by ACLED's
    // free-tier 12-month embargo.
    if (k.latest_month) {
      const [y, m] = k.latest_month.split('-').map(Number);
      const months = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
      const lbl = (m && y) ? `${months[m-1]} ${y}` : k.latest_month;
      set('latestMonthLabel', `EVENTS · ${lbl}`);
      set('latestMonthFoot', 'Most recent month available (ACLED ~12-mo embargo)');
    }
    // Find the specific event responsible for the peak move (largest single
    // 3-day price change in the event_table). Falls back to a generic label.
    const tbl = (resp && resp.event_table) || [];
    if (tbl.length) {
      const top = tbl.slice().sort((a, b) =>
        Math.abs((b.price_change_3d || 0)) - Math.abs((a.price_change_3d || 0))
      )[0];
      if (top && top.date) {
        const d = new Date(top.date);
        const lbl = isNaN(d) ? top.date : d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
        set('peak3dDate', lbl + (top.label ? ' · ' + String(top.label).slice(0, 40) : ''));
      }
    }
  }

  // ── MAIN LOAD ─────────────────────────────────────────────────────────────
  let resolveHydrated;
  window.CP = window.CP || {};
  window.CP.hydrated = new Promise(r => { resolveHydrated = r; });
  window.CP.state = {};

  // Bootstrap domain constants (RADIUS_KM, reference flows, war-onset date,
  // bounding boxes, etc.) from /api/constants. Runs first so every other
  // hydrate function can read window.CONSTANTS instead of hardcoding.
  // Falls back to in-file defaults if the endpoint is unreachable so the
  // page still renders during a backend outage.
  const _CONSTANTS_DEFAULTS = {
    anchors:               { hamas_attack: '2023-10-07', houthi_phase: '2023-12-01', war_onset: '2026-02-28' },
    chokepoint_flow_mbd:   { hormuz: 21.0, bab: 8.2, suez: 5.5 },
    global_liquids_mbd:    102.0,
    chokepoint_radius_km:  { hormuz: 650, bab: 400, suez: 300 },
    threat_tiers:          [{name:'critical',min_decline_pct:50},{name:'high',min_decline_pct:25},{name:'elevated',min_decline_pct:10},{name:'safe',min_decline_pct:0}],
  };
  window.CONSTANTS = window.CONSTANTS || _CONSTANTS_DEFAULTS;
  if (typeof API !== 'undefined' && API.constants) {
    API.constants()
      .then(c => { if (c) window.CONSTANTS = Object.assign({}, _CONSTANTS_DEFAULTS, c); })
      .catch(() => { /* defaults already in place */ });
  }

  // Fetch master with one retry — Render free tier sometimes drops the
  // connection mid-stream and a quick retry usually succeeds against the
  // now-warm in-memory cache.
  async function fetchMasterWithRetry() {
    try {
      const m = await API.master();
      if (m && m.timeseries && m.timeseries.length) return m;
      // Empty / malformed → retry once
      API.invalidate('/api/master');
      return await API.master();
    } catch (e) {
      try {
        API.invalidate('/api/master');
        await new Promise(r => setTimeout(r, 800));
        return await API.master();
      } catch (e2) {
        return null;
      }
    }
  }

  async function loadOnce() {
    const S = window.CP.state;
    const errors = [];

    // Health (non-blocking) — sets a generic "LIVE" pill. Immediately
    // overridden by hydrateFreshness() below as soon as the per-source
    // dates land, so the pill text reflects actual data freshness.
    API.health()
      .then(() => setConnectionStatus(true))
      .catch(() => setConnectionStatus(false));

    // Per-source freshness — replaces the generic pill with a date-stamped
    // honest summary (e.g. "MARKETS 30 MAR 2026 · EVENTS 16 MAR 2025").
    // Polled every 60s so refreshes update without a page reload.
    API.freshness()
      .then(snap => hydrateFreshness(snap))
      .catch(() => {});

    // Live HDX-mirrored ACLED monthly counts (no auth, ~7-day-old at worst).
    // Surfaces a "LIVE · APR 2026 · 312 events" caption on each chokepoint
    // card even when the live ACLED OAuth API is unreachable. After this
    // resolves the freshness pill auto-upgrades from "frozen" → "live"
    // because hdx_event_counts cache is now populated.
    API.liveEventCounts()
      .then(snap => {
        hydrateLiveEventCounts(snap);
        // Re-pull freshness so the pill picks up the now-cached HDX month.
        API.invalidate('/api/freshness');
        return API.freshness().then(s => hydrateFreshness(s));
      })
      .catch(() => {});

    // Parallel fetch of LIGHT endpoints — events is heavy (10MB+) so it's
    // fetched separately below and never blocks the hydrated promise.
    const [master, brent, iranResp, suez, bab, hormuz] = await Promise.all([
      fetchMasterWithRetry(),
      API.brent().then(r => r && r.data).catch(e => (errors.push(e), null)),
      API.iranEvents().catch(e => (errors.push(e), null)),
      API.suezTransits().then(r => r && r.data).catch(e => (errors.push(e), null)),
      API.babElMandeb().then(r => r && r.data).catch(e => (errors.push(e), null)),
      API.hormuzTransits().then(r => r && r.data).catch(e => (errors.push(e), null)),
    ]);

    // Stash raw responses for chart code (events arrives later, see below)
    S.master = master;
    S.brent  = brent;
    S.events = S.events || [];
    S.iran   = iranResp;
    S.suez   = suez;
    S.bab    = bab;
    S.hormuzTransits = hormuz;

    // ── Chokepoints ──
    // Real-world flow estimates (EIA) — these are geophysical constants,
    // not analytical values. Flows through a strait don't vary by the day.
    // Use whatever events we have so far; refine when /api/events resolves.
    // For Hormuz we also pass the Iran ACLED subset so the incident count
    // reflects IRGC / Iranian-coast activity that the regional feed omits.
    const iranEv = (iranResp && Array.isArray(iranResp.data)) ? iranResp.data : [];
    // EIA chokepoint reference flows (mbd) — sourced from window.CONSTANTS
    // (set by /api/constants on bootstrap). Falls back to the historic
    // EIA values if the constants endpoint hasn't resolved yet.
    const _flow = (window.CONSTANTS && window.CONSTANTS.chokepoint_flow_mbd) || {};
    hydrateChokepoint('hormuz', hormuz, S.events, _flow.hormuz != null ? _flow.hormuz : 21.0, iranEv);
    hydrateChokepoint('bab',    bab,    S.events, _flow.bab    != null ? _flow.bab    :  8.2);
    hydrateChokepoint('suez',   suez,   S.events, _flow.suez   != null ? _flow.suez   :  5.5);
    hydrateCape(hormuz);

    // ── Iran timeline events ──
    hydrateIranEvents(iranResp);

    // ── DOM updates ──
    hydrateHero(master, brent);
    hydrateSecondaryKpis(master, suez, hormuz, bab);
    hydrateThreatPills();
    hydrateChokepointCards();
    hydrateHeroFlowClaim();
    hydrateChokepointDecline();
    hydrateHeroBody();
    hydrateScenarios(brent);
    hydrateFeed(_mergeFeedItems(iranResp));

    // Hypothesis + GARCH (Analysis tab) — non-blocking; the cards show "—"
    // until this lands. Critical to fetch live: previous static markup carried
    // values opposite to the actual regression.
    API.hypothesis()
      .then(resp => { try { hydrateHypothesis(resp); hydrateGarch(resp); } catch (e) { console.warn('hypothesis render failed', e); } })
      .catch(() => {});

    // Iran-events first-fetch may have failed silently (Render free tier
    // cold-start, transient network). Schedule a single background retry
    // so the news feed and Iran timeline can populate without a manual
    // refresh. Retries when the merged feed is empty (covers both the
    // "news fetch failed" and "curated unavailable" cases).
    const _initialFeed = _mergeFeedItems(iranResp);
    if (!_initialFeed.length) {
      setTimeout(() => {
        API.invalidate('/api/iran-events');
        API.iranEvents()
          .then(r => {
            if (!r) return;
            S.iran = r;
            hydrateIranEvents(r);
            hydrateFeed(_mergeFeedItems(r));
            try { renderIranTimeline && renderIranTimeline(window.CP.state); } catch (_) {}
          })
          .catch(() => {});
      }, 4000);
    }

    // US-Iran tab KPIs (non-blocking; fetches independently)
    API.iranImpact()
      .then(r => hydrateIranKpis(r))
      .catch(() => hydrateIranKpis(null));

    // Market-context tiles (WTI-Brent spread, VIX, HY, US crude stocks).
    // All three endpoints are fetched in parallel and rendered independently
    // so a single slow endpoint can't hold up the overview.
    Promise.all([
      API.macroContext().catch(() => null),
      API.eiaInventories().catch(() => null),
    ]).then(([macro, inv]) => {
      try { hydrateMarketContext(macro, inv, brent); } catch (e) { console.warn('market-context render failed', e); }
    });

    // GDELT media tone — renders on US-Iran tab, non-blocking. On fetch
    // failure we still call renderGdeltChart(null) so the empty-state
    // overlay appears instead of leaving a blank rectangle.
    API.gdeltTone()
      .then(g => { try { renderGdeltChart(g); } catch (e) { console.warn('gdelt render failed', e); renderGdeltChart(null); } })
      .catch((e) => { console.warn('gdelt fetch failed', e); try { renderGdeltChart(null); } catch (_) {} });


    if (errors.length) {
      console.warn('Hydrator: partial failure —', errors);
      if (errors.length >= 4) {
        setConnectionStatus(false, 'OFFLINE · using cache', 'err');
      } else {
        setConnectionStatus(true, `DEGRADED · ${errors.length} source${errors.length === 1 ? '' : 's'} stale`, 'warn');
      }
    } else {
      setConnectionStatus(true, 'LIVE · ACLED + EIA', 'live');
    }

    // Resolve the gate IMMEDIATELY so charts can render with master/brent.
    // Heavy /api/events arrives in the background.
    window.dispatchEvent(new CustomEvent('data-hydrated', { detail: S }));
    if (resolveHydrated) { resolveHydrated(S); resolveHydrated = null; }

    // ── Background fetch: /api/events (heavy ACLED payload) ──
    API.events()
      .then(r => (r && r.data) || [])
      .catch(() => [])
      .then(events => {
        S.events = events;
        // Refresh chokepoint incident counts now that real events are in.
        // Per-chokepoint radii match the module-level RADIUS_KM constant.
        //
        // PREFER the merged-pool counts written by app.js's
        // refreshSidebarIncidents (which folds in curated war timeline +
        // news so the count reflects real recent oil-impactful activity).
        // Fall back to raw ACLED+iran only if the merged pool hasn't
        // landed yet — this avoids the race where /api/events finishes
        // last and overwrites the merged count back to ~0 under ACLED's
        // 12-month embargo.
        const iranEv2 = (S.iran && Array.isArray(S.iran.data)) ? S.iran.data : [];
        ['hormuz', 'bab', 'suez'].forEach(k => {
          const cp = window.CHOKEPOINTS[k];
          if (!cp) return;
          const merged = (window.CP && window.CP.incidents && window.CP.incidents[k] &&
                          Array.isArray(window.CP.incidents[k].events))
                        ? window.CP.incidents[k].events
                        : null;
          if (merged && merged.length) {
            // Merged events are already chokepoint-filtered + within 90d
            // wall-clock. Just count those within 30d.
            const cutoff30 = Date.now() - 30 * 86400000;
            let n = 0;
            for (const ev of merged) {
              const t = Date.parse(ev.date || ev.event_date || '');
              if (isFinite(t) && t >= cutoff30) n++;
            }
            cp.incidents30d = n;
          } else {
            const evForCount = eventsForChokepoint(k, events, iranEv2);
            cp.incidents30d = countRecentIncidents(evForCount, cp, RADIUS_KM_for(k), 30);
          }
        });
        // Hero "INCIDENTS · 30D" KPI is owned by app.js's refreshHeroIncidentsKpi
        // — it sources from the deduped curated + iran-news union (the same pool
        // that feeds §11 and §09). Earlier this block wrote the naive
        // Hormuz+Bab sum here, which (a) double-counted overlapping events
        // after the keyword scope was broadened and (b) created a flicker:
        // refreshHeroIncidentsKpi would land "59" via data-hydrated, then
        // /api/events resolved a moment later and this line overwrote it
        // with "32". Delegate to the canonical setter instead so there's
        // exactly one source of truth.
        if (typeof window.__refreshHeroIncidentsKpi === 'function') {
          try { window.__refreshHeroIncidentsKpi(); } catch (_) {}
        }
        // Refresh per-card metrics + threat pills + tac-side stats now that real counts exist
        hydrateChokepointCards();
        hydrateThreatPills();
        hydrateTacStats(events);
        // Update hotspots layer for globe
        window.ATTACKS = buildAttackHotspots(events);
        window.dispatchEvent(new CustomEvent('events-ready', { detail: { events } }));
      });

    // ── Thesis events (optional) ──
    API.thesisEvents()
      .then(r => { window.THESIS_EVENTS = (r && r.data) || []; window.dispatchEvent(new CustomEvent('thesis-events-ready')); })
      .catch(() => {});
  }

  // ── Kick off ──────────────────────────────────────────────────────────────
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', loadOnce);
  } else {
    loadOnce();
  }

  // ── Auto-refresh every 5 minutes ──────────────────────────────────────────
  setInterval(() => {
    API.invalidate();
    loadOnce().catch(e => console.error('Refresh failed:', e));
  }, 5 * 60 * 1000);

  // ── Freshness re-poll every 60s (lightweight) ──────────────────────────────
  // Independent of the heavy 5-min reload so the status pill stays honest
  // without re-hitting /api/master + /api/events.
  setInterval(() => {
    API.invalidate('/api/freshness');
    API.freshness()
      .then(snap => hydrateFreshness(snap))
      .catch(() => {});
  }, 60 * 1000);

  // Expose manual refresh for console use
  window.CP.refresh = () => { API.invalidate(); return loadOnce(); };
})();
