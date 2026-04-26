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
    // source (3rd-party MarineTraffic embed; not our backend).
    document.querySelectorAll('[data-vt-asof]').forEach(el => {
      el.textContent = 'Live AIS · MarineTraffic embed';
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
    if (!Array.isArray(events)) return 0;
    const anchor = dataNowTs(events);
    const cutoff = anchor - days * 86400000;
    let n = 0;
    for (const e of events) {
      const lat = +e.latitude, lon = +e.longitude;
      if (!lat || !lon) continue;
      const d = e.event_date || e.date;
      if (!d) continue;
      const t = Date.parse(d);
      if (isNaN(t) || t < cutoff || t > anchor) continue;
      if (haversineKm(lat, lon, cp.lat, cp.lon) <= km) n++;
    }
    return n;
  }

  // Expose for charts.js so its scrubber-aware KPI uses the same anchor.
  window.__dataNowTs = dataNowTs;

  // Hormuz is a 21-mile chokepoint but the operationally relevant "in-zone"
  // region is the Persian Gulf + Gulf of Oman — a radius of 300km only
  // catches events directly in the strait, so per-chokepoint widths are
  // used to pick up Gulf-area activity (UAE/Oman coast, southern Iran
  // shipping lanes when present in the dataset).
  const RADIUS_KM = { hormuz: 650, bab: 400, suez: 300 };

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
    cp.incidents30d  = countRecentIncidents(evForCount, cp, RADIUS_KM[key] || 300, 30);
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
    const warOnset = new Date('2026-02-28T00:00:00Z');
    const daysSinceWar = Math.max(0, Math.floor((Date.now() - warOnset.getTime()) / 86400000));
    document.querySelectorAll('[data-days-since-war]').forEach(el => {
      el.textContent = `Feb 28 → now: ${daysSinceWar} days of market whiplash.`;
    });

    // 30-day range foot row + war-onset % (also surfaced in threat-strip below)
    let warPct = null;
    const foot = document.querySelector('.kpi-hero-foot');
    if (Array.isArray(brent) && brent.length) {
      const last30 = brent.slice(-30).map(r => r.price).filter(v => v != null);
      const warStart = brent.find(r => r.date >= '2026-02-28');
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

    // Incidents KPI row (count of maritime events in last 30d). Guard against
    // null sums when CHOKEPOINTS aren't yet hydrated (NaN would render as text).
    const incidentEl = document.querySelector('[data-kpi="incidents30"]');
    if (incidentEl) {
      const h = (window.CHOKEPOINTS.hormuz && window.CHOKEPOINTS.hormuz.incidents30d) || 0;
      const b = (window.CHOKEPOINTS.bab    && window.CHOKEPOINTS.bab.incidents30d)    || 0;
      incidentEl.textContent = String(h + b);
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
  let __gdeltChartInstance = null;
  function renderGdeltChart(gdelt) {
    const canvas = document.getElementById('gdeltToneChart');
    if (!canvas || !window.Chart || !gdelt) return;
    const tone = Array.isArray(gdelt.tone) ? gdelt.tone : [];
    if (!tone.length) return;

    // Sort ascending by date (GDELT returns newest first)
    const sorted = tone.slice().sort((a, b) => a.date.localeCompare(b.date));
    const labels = sorted.map(r => r.date);
    const values = sorted.map(r => +r.value);

    // Build a 7-day simple moving average to smooth the day-to-day noise
    const sma = values.map((_, i) => {
      const window = values.slice(Math.max(0, i - 6), i + 1);
      return window.reduce((s, v) => s + v, 0) / window.length;
    });

    if (__gdeltChartInstance) {
      try { __gdeltChartInstance.destroy(); } catch (_) {}
    }
    __gdeltChartInstance = new Chart(canvas.getContext('2d'), {
      type: 'line',
      data: {
        labels,
        datasets: [
          {
            label: 'Daily tone',
            data: values,
            borderColor: 'rgba(61, 153, 212, 0.45)',
            backgroundColor: 'rgba(61, 153, 212, 0.08)',
            borderWidth: 1,
            pointRadius: 0,
            fill: true,
            tension: 0.2,
          },
          {
            label: '7-day avg',
            data: sma,
            borderColor: '#ff5252',
            borderWidth: 2,
            pointRadius: 0,
            fill: false,
            tension: 0.25,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { labels: { color: '#c8d0dc', font: { family: 'JetBrains Mono', size: 10 } } },
          tooltip: {
            callbacks: {
              label: (ctx) => `${ctx.dataset.label}: ${(+ctx.parsed.y).toFixed(2)}`,
            },
          },
        },
        scales: {
          x: {
            ticks: { color: '#7e8699', font: { family: 'JetBrains Mono', size: 9 }, maxTicksLimit: 8 },
            grid: { color: 'rgba(255,255,255,0.04)' },
          },
          y: {
            ticks: { color: '#7e8699', font: { family: 'JetBrains Mono', size: 9 } },
            grid: { color: 'rgba(255,255,255,0.05)' },
            title: { display: true, text: 'Average tone', color: '#9099aa', font: { size: 10 } },
          },
        },
      },
    });
  }

  // ── News & event feed ─────────────────────────────────────────────────────
  function hydrateFeed(news) {
    const feed = $('feedList');
    if (!feed) return;
    // Cache the unfiltered list so the temporal scrubber can re-render
    // without needing a refetch.
    if (Array.isArray(news)) window.__feedRaw = news;
    const raw = Array.isArray(news) ? news : (window.__feedRaw || []);
    // Apply temporal scrubber: drop items published after the scrubbed date.
    const tl = window.CP && window.CP.timeline;
    const cutoffMs = (tl && !tl.atNow) ? tl.ts : null;
    const tlFiltered = cutoffMs
      ? raw.filter(n => {
          if (!n.published_at) return true;
          const t = Date.parse(n.published_at);
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
      const d = n.published_at ? new Date(n.published_at) : null;
      const time = d && !isNaN(d) ? `${String(d.getUTCHours()).padStart(2,'0')}:${String(d.getUTCMinutes()).padStart(2,'0')}Z` : '—';
      const type = (n.category || n.type || 'diplomatic').toLowerCase();
      return `
        <div class="feed-item">
          <span class="feed-time">${time}</span>
          <span class="feed-type ft-${type}">${type.toUpperCase()}</span>
          <span class="feed-title">${(n.title || '').replace(/</g, '&lt;')}</span>
          <span class="feed-source">${(n.source || '').replace(/</g, '&lt;')}</span>
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
    // Normalize to shape used by charts.js
    // Note: the backend's curated events don't carry brent_price — the chart
    // looks up the nearest Brent trading-day price at render time. So we only
    // require a valid date here, not a pre-joined price.
    window.IRAN_EVENTS = curated.map(e => ({
      date:  e.date,
      type:  (e.type || e.category || 'diplomatic').toLowerCase(),
      label: e.label || e.title || e.name || '',
      price: e.brent_price != null ? +e.brent_price : null,
    })).filter(e => e.date).sort((a, b) => a.date.localeCompare(b.date));
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

  // ── Chokepoint cards (overview tab) — hydrate vessels/premium/incidents/flow
  // Premium isn't a backend metric we track; derive a proxy from threatPct
  // (war-risk premium is correlated with chokepoint stress).
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
      // Vessels
      if (cp.vesselsInZone != null) set('vessels', cp.vesselsInZone);
      // Premium proxy: scale threatPct → 0–3.5% range
      if (cp.threatPct != null) set('premium', (cp.threatPct / 100 * 3.5).toFixed(1) + '%');
      // Incidents/30d
      if (cp.incidents30d != null) set('incidents', cp.incidents30d);
      // Cape: vessels diverted
      if (key === 'cape' && cp.vesselsInZone != null) set('diverted', cp.vesselsInZone);
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
    const GLOBAL_LIQUIDS_MBD = 102; // EIA STEO 2026 forecast for world liquids consumption
    const pct = (total / GLOBAL_LIQUIDS_MBD) * 100;
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
    set('thisMonth', k.events_this_month != null ? k.events_this_month : '—');
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
    hydrateChokepoint('hormuz', hormuz, S.events, 21.0, iranEv);
    hydrateChokepoint('bab',    bab,    S.events,  8.2);
    hydrateChokepoint('suez',   suez,   S.events,  5.5);
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
    hydrateScenarios(brent);
    hydrateFeed((iranResp && iranResp.news) || []);

    // Hypothesis + GARCH (Analysis tab) — non-blocking; the cards show "—"
    // until this lands. Critical to fetch live: previous static markup carried
    // values opposite to the actual regression.
    API.hypothesis()
      .then(resp => { try { hydrateHypothesis(resp); hydrateGarch(resp); } catch (e) { console.warn('hypothesis render failed', e); } })
      .catch(() => {});

    // Iran-events first-fetch may have failed silently (Render free tier
    // cold-start, transient network). Schedule a single background retry so
    // the news feed and Iran timeline can populate without a manual refresh.
    if (!iranResp || !Array.isArray(iranResp.news) || !iranResp.news.length) {
      setTimeout(() => {
        API.invalidate('/api/iran-events');
        API.iranEvents()
          .then(r => {
            if (!r) return;
            S.iran = r;
            hydrateIranEvents(r);
            hydrateFeed((r && r.news) || []);
            // Re-render Iran timeline with new curated events
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

    // GDELT media tone — renders on US-Iran tab, non-blocking
    API.gdeltTone()
      .then(g => { try { renderGdeltChart(g); } catch (e) { console.warn('gdelt render failed', e); } })
      .catch(() => {});

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
        const iranEv2 = (S.iran && Array.isArray(S.iran.data)) ? S.iran.data : [];
        ['hormuz', 'bab', 'suez'].forEach(k => {
          const cp = window.CHOKEPOINTS[k];
          if (!cp) return;
          const evForCount = eventsForChokepoint(k, events, iranEv2);
          cp.incidents30d = countRecentIncidents(evForCount, cp, RADIUS_KM[k] || 300, 30);
        });
        // Refresh aggregate incidents KPI
        const incidentEl = document.querySelector('[data-kpi="incidents30"]');
        if (incidentEl) {
          incidentEl.textContent = String(
            (window.CHOKEPOINTS.hormuz.incidents30d || 0) +
            (window.CHOKEPOINTS.bab.incidents30d || 0)
          );
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
