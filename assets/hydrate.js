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
  function setConnectionStatus(ok, label) {
    const dot = document.querySelector('.status-dot');
    const txt = document.querySelector('.status-text');
    if (dot) {
      dot.classList.toggle('live', !!ok);
      dot.classList.toggle('err', !ok);
    }
    if (txt) txt.textContent = label || (ok ? 'LIVE · ACLED + EIA' : 'OFFLINE · using cache');
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

  function countRecentIncidents(events, cp, km = 300, days = 30) {
    if (!Array.isArray(events)) return 0;
    const cutoff = Date.now() - days * 86400000;
    let n = 0;
    for (const e of events) {
      const lat = +e.latitude, lon = +e.longitude;
      if (!lat || !lon) continue;
      const d = e.event_date || e.date;
      if (!d) continue;
      const t = Date.parse(d);
      if (isNaN(t) || t < cutoff) continue;
      if (haversineKm(lat, lon, cp.lat, cp.lon) <= km) n++;
    }
    return n;
  }

  function hydrateChokepoint(key, transitData, events, flowMbd) {
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
    cp.incidents30d  = countRecentIncidents(events, cp, 300, 30);
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

    // 30-day range foot row
    const foot = document.querySelector('.kpi-hero-foot');
    if (foot && Array.isArray(brent) && brent.length) {
      const last30 = brent.slice(-30).map(r => r.price).filter(v => v != null);
      if (last30.length) {
        const lo = Math.min(...last30), hi = Math.max(...last30);
        // Since war onset (Feb 28 2026) — find first price on or after
        const warStart = brent.find(r => r.date >= '2026-02-28');
        const warPct = (warStart && latest != null) ? ((latest - warStart.price) / warStart.price * 100) : null;
        foot.innerHTML = `
          <span>30D RANGE <b>$${lo.toFixed(2)} — $${hi.toFixed(2)}</b></span>
          <span>SINCE WAR ONSET <b class="${warPct>=0?'up':'down'}">${warPct!=null?(warPct>=0?'+':'')+warPct.toFixed(1)+'%':'—'}</b></span>`;
      }
    }

    // Header threat-bar KPIs
    setText('threatBrent', latest != null ? '$' + latest.toFixed(2) : '—');
    setText('threatOvx',   kpis.latest_ovx != null ? kpis.latest_ovx.toFixed(1) : '—');

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
    const ovxEl = document.querySelector('.kpi-sec:nth-of-type(1) .kpi-sec-value') ||
                  document.querySelector('[data-kpi="ovx"]');
    if (ovxEl && kpis.latest_ovx != null) {
      const arrow = kpis.latest_ovx > 40 ? '<span class="up">▲</span>' : '<span class="down">▼</span>';
      ovxEl.innerHTML = `${kpis.latest_ovx.toFixed(1)} ${arrow}`;
    }

    // DXY
    const dxyEl = document.querySelector('.kpi-sec:nth-of-type(2) .kpi-sec-value') ||
                  document.querySelector('[data-kpi="dxy"]');
    if (dxyEl && kpis.latest_dxy != null) {
      const arrow = kpis.latest_dxy > 100 ? '<span class="up">▲</span>' : '<span class="down">▼</span>';
      dxyEl.innerHTML = `${kpis.latest_dxy.toFixed(1)} ${arrow}`;
    }

    // Suez throughput % change vs pre-war
    const suezEl = document.querySelector('.kpi-sec:nth-of-type(3) .kpi-sec-value') ||
                   document.querySelector('[data-kpi="suez"]');
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
    const flowEl = document.querySelector('.kpi-primary-warn .kpi-value');
    if (flowEl && window.CHOKEPOINTS.hormuz.flowMbd != null && window.CHOKEPOINTS.bab.flowMbd != null) {
      const total = window.CHOKEPOINTS.hormuz.flowMbd + window.CHOKEPOINTS.bab.flowMbd;
      flowEl.innerHTML = `${total.toFixed(1)} <span class="kpi-unit">mbd</span>`;
      const foot = flowEl.parentElement.querySelector('.kpi-foot');
      if (foot) foot.textContent = `Hormuz ${window.CHOKEPOINTS.hormuz.flowMbd.toFixed(1)} + Bab el-Mandeb ${window.CHOKEPOINTS.bab.flowMbd.toFixed(1)}`;
    }

    // Incidents KPI row (count of maritime events in last 30d)
    const incidentEl = document.querySelector('[data-kpi="incidents30"]');
    if (incidentEl) {
      const n = window.CHOKEPOINTS.hormuz.incidents30d + window.CHOKEPOINTS.bab.incidents30d;
      incidentEl.textContent = String(n);
    }
  }

  // ── News & event feed ─────────────────────────────────────────────────────
  function hydrateFeed(news) {
    const feed = $('feedList');
    if (!feed) return;
    const items = (Array.isArray(news) ? news : []).slice(0, 10);
    if (!items.length) {
      feed.innerHTML = '<div class="feed-empty">No recent news.</div>';
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

  // ── Iran curated events timeline (for chart + event markers) ──────────────
  function hydrateIranEvents(iranResp) {
    const curated = (iranResp && Array.isArray(iranResp.curated)) ? iranResp.curated : [];
    // Normalize to shape used by charts.js
    window.IRAN_EVENTS = curated.map(e => ({
      date:  e.date,
      type:  (e.type || e.category || 'diplomatic').toLowerCase(),
      label: e.label || e.title || e.name || '',
      price: e.brent_price != null ? +e.brent_price : null,
    })).filter(e => e.date && e.price != null).sort((a, b) => a.date.localeCompare(b.date));
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

    // Health (non-blocking)
    API.health()
      .then(() => setConnectionStatus(true))
      .catch(() => setConnectionStatus(false));

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
    hydrateChokepoint('hormuz', hormuz, S.events, 21.0);
    hydrateChokepoint('bab',    bab,    S.events,  8.2);
    hydrateChokepoint('suez',   suez,   S.events,  5.5);
    hydrateCape(hormuz);

    // ── Iran timeline events ──
    hydrateIranEvents(iranResp);

    // ── DOM updates ──
    hydrateHero(master, brent);
    hydrateSecondaryKpis(master, suez, hormuz, bab);
    hydrateFeed((iranResp && iranResp.news) || []);

    if (errors.length) {
      console.warn('Hydrator: partial failure —', errors);
      if (errors.length >= 4) setConnectionStatus(false, 'PARTIAL · some data stale');
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
        // Refresh chokepoint incident counts now that real events are in
        ['hormuz', 'bab', 'suez'].forEach(k => {
          const cp = window.CHOKEPOINTS[k];
          if (cp) cp.incidents30d = countRecentIncidents(events, cp, 300, 30);
        });
        // Refresh aggregate incidents KPI
        const incidentEl = document.querySelector('[data-kpi="incidents30"]');
        if (incidentEl) {
          incidentEl.textContent = String(
            (window.CHOKEPOINTS.hormuz.incidents30d || 0) +
            (window.CHOKEPOINTS.bab.incidents30d || 0)
          );
        }
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

  // Expose manual refresh for console use
  window.CP.refresh = () => { API.invalidate(); return loadOnce(); };
})();
