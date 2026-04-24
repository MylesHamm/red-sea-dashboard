/* ═══════════════════════════════════════════════════════════════════════════
   CHOKEPOINT INTEL — API Client
   Wraps the FastAPI backend (see app.py). All data is fetched live.
   No hardcoded analytical values. Base URL is inferred from window.location
   so the same file works in dev (localhost:8000) and in prod (Render).
   ═══════════════════════════════════════════════════════════════════════════ */

window.API = (function () {
  // Backend base URL. Override by setting window.__API_BASE before this script loads.
  const BASE = window.__API_BASE || '';

  const cache = new Map();          // in-memory cache for this session
  const inflight = new Map();       // dedupe concurrent requests

  async function _fetch(path, { ttlMs = 0, signal } = {}) {
    const url = BASE + path;
    const now = Date.now();
    const hit = cache.get(path);
    if (hit && ttlMs && (now - hit.ts) < ttlMs) return hit.data;
    if (inflight.has(path)) return inflight.get(path);

    const p = fetch(url, { signal, headers: { 'Accept': 'application/json' } })
      .then(async r => {
        if (!r.ok) throw new Error(`${path} → HTTP ${r.status}`);
        const json = await r.json();
        cache.set(path, { data: json, ts: Date.now() });
        return json;
      })
      .finally(() => inflight.delete(path));

    inflight.set(path, p);
    return p;
  }

  function invalidate(path) {
    if (path) cache.delete(path); else cache.clear();
  }

  // ── Endpoints (mirror app.py) ──
  return {
    BASE,
    invalidate,
    health:           ()  => _fetch('/api/health'),
    master:           ()  => _fetch('/api/master',               { ttlMs: 60_000 }),
    events:           ()  => _fetch('/api/events',               { ttlMs: 60_000 }),
    // Heavy payload with `notes` — only used by Data Explorer search.
    eventsFull:       ()  => _fetch('/api/events?lite=0',         { ttlMs: 300_000 }),
    thesisEvents:     ()  => _fetch('/api/thesis-events',        { ttlMs: 300_000 }),
    brent:            ()  => _fetch('/api/brent',                { ttlMs: 60_000 }),
    dxy:              ()  => _fetch('/api/dxy',                  { ttlMs: 60_000 }),
    ovx:              ()  => _fetch('/api/ovx',                  { ttlMs: 60_000 }),
    spr:              ()  => _fetch('/api/spr',                  { ttlMs: 300_000 }),
    chinaPmi:         ()  => _fetch('/api/china-pmi',            { ttlMs: 300_000 }),
    hypothesis:       ()  => _fetch('/api/hypothesis',           { ttlMs: 600_000 }),
    suezTransits:     ()  => _fetch('/api/suez-transits',        { ttlMs: 300_000 }),
    babElMandeb:      ()  => _fetch('/api/bab-el-mandeb-transits',{ttlMs: 300_000 }),
    hormuzTransits:   ()  => _fetch('/api/hormuz-transits',      { ttlMs: 300_000 }),
    iranEvents:       ()  => _fetch('/api/iran-events',          { ttlMs: 60_000 }),
    iranImpact:       ()  => _fetch('/api/iran-impact',          { ttlMs: 60_000 }),
    comparative:      ()  => _fetch('/api/comparative',          { ttlMs: 300_000 }),
    iranIntensity:    ()  => _fetch('/api/iran-intensity',       { ttlMs: 300_000 }),
    hormuzStatus:     ()  => _fetch('/api/hormuz-status',        { ttlMs: 300_000 }),
    warPhases:        ()  => _fetch('/api/war-phases',           { ttlMs: 3_600_000 }),
    iranRegression:   ()  => _fetch('/api/iran-regression',      { ttlMs: 600_000 }),
    refresh:          ()  => fetch(BASE + '/api/refresh', { method: 'POST' }).then(r => r.json()),
    refreshStatus:    ()  => _fetch('/api/refresh-status'),
    // Tier-1 free APIs (EIA inventories + FRED macro + GDELT media tone)
    eiaInventories:   ()  => _fetch('/api/eia-inventories',     { ttlMs: 900_000 }),
    macroContext:     ()  => _fetch('/api/macro-context',       { ttlMs: 300_000 }),
    gdeltTone:        ()  => _fetch('/api/gdelt-tone',          { ttlMs: 1_800_000 }),
    // Live AIS — short TTL because vessels move in real time
    vessels:          (cp) => _fetch(`/api/vessels/${encodeURIComponent(cp)}`, { ttlMs: 15_000 }),
    vesselsStatus:    ()   => _fetch('/api/vessels-status', { ttlMs: 15_000 }),
  };
})();

// ── Helpers for chart/UI code ──
window.APIUtil = {
  fmtPrice(v, d = 2) { return (v == null || isNaN(v)) ? '—' : '$' + Number(v).toFixed(d); },
  fmtNum(v, d = 1)   { return (v == null || isNaN(v)) ? '—' : Number(v).toFixed(d); },
  fmtDelta(v, d = 2) {
    if (v == null || isNaN(v)) return '—';
    const n = Number(v);
    const sign = n > 0 ? '▲ +' : (n < 0 ? '▼ ' : '');
    return `${sign}${n.toFixed(d)}`;
  },
  pctChange(cur, prev) {
    if (cur == null || prev == null || prev === 0) return null;
    return ((cur - prev) / prev) * 100;
  },
  // Pull last N daily points (assumes ascending date order)
  tail(arr, n) { return Array.isArray(arr) ? arr.slice(Math.max(0, arr.length - n)) : []; },
  // Find most recent non-null value of `key`
  lastVal(arr, key = 'value') {
    if (!Array.isArray(arr)) return null;
    for (let i = arr.length - 1; i >= 0; i--) {
      const v = arr[i] && arr[i][key];
      if (v != null && !isNaN(v)) return Number(v);
    }
    return null;
  },
};
