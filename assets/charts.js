/* ═══════════════════════════════════════════════════════════════════════════
   CHOKEPOINT INTEL — Charts
   All charts are rendered AFTER window.CP.hydrated resolves so they use
   live data from the backend. No synthetic series.
   ═══════════════════════════════════════════════════════════════════════════ */

Chart.defaults.color = '#8f9db0';
Chart.defaults.borderColor = 'rgba(0,212,255,0.08)';
Chart.defaults.font.family = "'JetBrains Mono', ui-monospace, monospace";
Chart.defaults.font.size = 10;

const C_CYAN = '#00d4ff', C_RED = '#ff3d5e', C_AMBER = '#ffab00',
      C_ORANGE = '#ff8c42', C_GREEN = '#00e690', C_PURPLE = '#9b7bee',
      C_BLUE = '#3d9cff', C_GOLD = '#d4a843';

// ── Helpers ────────────────────────────────────────────────────────────────
function chartEl(id) { return document.getElementById(id); }

// ── Oil-relevance filter (ACLED event types) ───────────────────────────────
// The dashboard's analytical purpose is OIL PRICE IMPACT. ACLED publishes
// every conflict event regardless of oil-market relevance. Civilian
// protests and riots — even in Yemen / Iran — don't move oil markets and
// shouldn't appear in any chart titled "Attacks vs ...", "Event Type
// Mix", "Brent vs US-Iran Events", etc.
//
// Excluded ACLED event_types:
//   • Protests           — peaceful demonstrations (no supply impact)
//   • Riots              — civilian civil unrest (no supply impact)
//
// Kept (oil-relevant or geopolitically material):
//   • Battles                       — military engagements
//   • Explosions/Remote violence    — strikes, missile, drones, mining
//   • Violence against civilians    — proxy for conflict intensity
//   • Strategic developments        — political moves (sanctions, agreements)
//
// This list is the SAME filter applied server-side in
// data_service.get_chokepoint_incidents and frontend countRecentIncidents,
// so every chart that calls oilRelevant() gets a consistent universe.
const _OIL_IRRELEVANT_TYPES = new Set(['protests', 'riots']);
function oilRelevant(events) {
  if (!Array.isArray(events)) return [];
  return events.filter(e => !_OIL_IRRELEVANT_TYPES.has((e && e.event_type || '').toLowerCase()));
}
window.oilRelevant = oilRelevant;

// ── Temporal cutoff helpers (driven by app.js scrubber → window.CP.timeline)
//    All timeseries-driven render functions filter rows by this so scrubbing
//    truncates the chart to <= the scrubbed date.
function tlCutoffDate() {
  const t = window.CP && window.CP.timeline;
  if (!t || t.atNow) return null;          // no filter at "now"
  return t.date || null;
}
function tlCutoffTs() {
  const t = window.CP && window.CP.timeline;
  if (!t || t.atNow) return null;
  return t.ts || null;
}
function applyTimelineRows(rows) {
  const c = tlCutoffDate();
  if (!c || !rows || !rows.length) return rows || [];
  return rows.filter(r => r.date && r.date <= c);
}
// Destroy any Chart bound to this canvas so re-renders don't error with
// "Canvas is already in use". Also strip any prior .chart-empty overlay so
// a successful re-render isn't obscured by a stale "unavailable" message.
function destroyChartOn(canvas) {
  if (!canvas) return;
  if (typeof Chart !== 'undefined' && Chart.getChart) {
    const prior = Chart.getChart(canvas);
    if (prior) prior.destroy();
  }
  const parent = canvas.parentElement;
  const overlay = parent && parent.querySelector('.chart-empty');
  if (overlay) overlay.remove();
}

function showChartEmpty(canvas, msg) {
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const parent = canvas.parentElement;
  if (parent && !parent.querySelector('.chart-empty')) {
    const ov = document.createElement('div');
    ov.className = 'chart-empty';
    ov.textContent = msg || 'No data available';
    ov.style.cssText = 'position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#556475;font:11px \'JetBrains Mono\',monospace;letter-spacing:2px';
    if (getComputedStyle(parent).position === 'static') parent.style.position = 'relative';
    parent.appendChild(ov);
  }
}

// ── Cross-filter taxonomy bridge ────────────────────────────────────────────
// The doughnut publishes ACLED categories ("Battles", "Explosions/Remote
// violence", "Strategic developments", "Violence against civilians",
// "Protests", "Riots"). Other consumers use their own taxonomies; map them.
function xfActiveType() {
  return (window.CP && window.CP.filters && window.CP.filters.eventType) || null;
}
function xfMatchesIranType(filter, t) {
  if (!filter) return true;
  const f = filter.toLowerCase();
  t = (t || '').toLowerCase();
  if (f.includes('explosion') || f.includes('battle'))   return t === 'military' || t === 'proxy';
  if (f.includes('strategic'))                            return t === 'nuclear' || t === 'sanctions';
  if (f.includes('violence'))                             return t === 'military' || t === 'proxy';
  return true; // unknown filter — leave all visible
}
function xfMatchesTacType(filter, t) {
  if (!filter) return true;
  const f = filter.toLowerCase();
  t = (t || '').toLowerCase();
  if (f.includes('explosion') || f.includes('battle')) return t === 'missile' || t === 'drone';
  if (f.includes('violence'))                          return t === 'tanker';
  if (f.includes('strategic'))                         return false; // none on the tac map
  return true;
}
function xfMatchesNewsCat(filter, c) {
  if (!filter) return true;
  const f = filter.toLowerCase();
  c = (c || '').toLowerCase();
  if (f.includes('explosion') || f.includes('battle')) return /military|attack|conflict|incident|strike/.test(c);
  if (f.includes('strategic'))                         return /diplomatic|sanction|treaty|nuclear|policy/.test(c);
  if (f.includes('violence'))                          return /military|conflict|attack|civilian/.test(c);
  return true;
}

// Generic floating filter banner used by consumers in different taxonomies
// that can't natively re-aggregate by ACLED type — they show a "FILTER ACTIVE"
// pill so the user knows the cross-filter is shaping the chart.
function showXfBanner(canvas, label) {
  if (!canvas) return;
  const parent = canvas.parentElement;
  if (!parent) return;
  if (getComputedStyle(parent).position === 'static') parent.style.position = 'relative';
  let pill = parent.querySelector('.xf-banner');
  if (!label) {
    if (pill) pill.remove();
    return;
  }
  if (!pill) {
    pill = document.createElement('div');
    pill.className = 'xf-banner';
    pill.style.cssText = 'position:absolute;top:8px;right:8px;padding:3px 7px;font:9px "JetBrains Mono",monospace;letter-spacing:1.5px;border-radius:3px;background:rgba(0,212,255,0.15);color:#dfe7f0;border:1px solid rgba(0,212,255,0.45);z-index:5;pointer-events:none';
    parent.appendChild(pill);
  }
  pill.textContent = `FILTER: ${label.toUpperCase()}`;
}

// ── Main render pipeline ───────────────────────────────────────────────────
async function renderAllCharts() {
  // Wait for hydrator — tolerate up to 15s, then render with whatever we have
  let state = {};
  try {
    state = await Promise.race([
      window.CP.hydrated,
      new Promise((_, rj) => setTimeout(() => rj(new Error('hydrate-timeout')), 15000))
    ]);
  } catch (e) {
    console.warn('Charts: hydrator did not resolve in time, using empty state.');
    state = window.CP && window.CP.state || {};
  }

  renderPriceVsAttacks(state);
  renderVolatilityChart(state);
  renderEventTypesChart(state);
  renderPriceWindow(state);
  renderScatter(state);
  renderDxyOvx(state);
  renderCorrelation(state);
  renderIranTimeline(state);
}

document.addEventListener('DOMContentLoaded', renderAllCharts);

// ═══════════════════════════════════════════════════════════════════════════
// Price × Weekly Attacks (Overview tab)
// ═══════════════════════════════════════════════════════════════════════════
function renderPriceVsAttacks(state) {
  const canvas = chartEl('priceAttackChart');
  if (!canvas) return;
  destroyChartOn(canvas);
  const xfType = xfActiveType();
  showXfBanner(canvas, xfType);

  const ts = applyTimelineRows((state.master && state.master.timeseries) || []);
  if (!ts.length) return showChartEmpty(canvas, 'timeseries unavailable');

  // Weekly: sample every ~7 days from master timeseries
  const step = Math.max(1, Math.floor(ts.length / 78)); // ~78 weeks max
  const rows = [];
  for (let i = 0; i < ts.length; i += step) rows.push(ts[i]);
  // Keep the most recent point regardless
  if (rows[rows.length - 1] !== ts[ts.length - 1]) rows.push(ts[ts.length - 1]);

  const labels   = rows.map(r => r.date);
  const prices   = rows.map(r => (r.brent_price != null ? +r.brent_price : null));

  // Weekly attack counts. When the cross-filter is inactive, use the backend's
  // pre-aggregated `weekly_attacks` column from master.timeseries. When active,
  // recompute per-week counts from raw ACLED events using the same bucketing
  // (Monday-anchored ISO week), filtered to only the selected event_type.
  // This lets the red bars actually SHRINK to the filtered subset instead of
  // just showing a "FILTER: X" banner over the full-dataset bars.
  let attacks;
  if (xfType && Array.isArray(state.events) && state.events.length) {
    // §01 is "Price vs. Conflict Intensity" — oil-relevance filter the
    // cross-filtered count too. Without this, selecting "Protests" in
    // §03 (which after the filter has nothing to select anyway) would
    // count irrelevant events as bars on §01.
    const xfEvents = oilRelevant(state.events);
    const counts = new Map();
    for (const e of xfEvents) {
      const t = (e.event_type || e.sub_event_type || 'Other').toString();
      if (t !== xfType) continue;
      const d = e.event_date;
      if (!d) continue;
      // Snap to ISO week-start (Monday)
      const dt = new Date(d);
      if (isNaN(dt)) continue;
      const day = dt.getUTCDay() || 7;      // Sun=0 → 7
      const monday = new Date(dt); monday.setUTCDate(dt.getUTCDate() - (day - 1));
      const key = monday.toISOString().slice(0, 10);
      counts.set(key, (counts.get(key) || 0) + 1);
    }
    // For each sampled row, take the count of events in the week ending on/before its date
    attacks = rows.map(r => {
      if (!r.date) return 0;
      const dt = new Date(r.date);
      if (isNaN(dt)) return 0;
      const day = dt.getUTCDay() || 7;
      const monday = new Date(dt); monday.setUTCDate(dt.getUTCDate() - (day - 1));
      const key = monday.toISOString().slice(0, 10);
      return counts.get(key) || 0;
    });
  } else {
    attacks = rows.map(r => (r.weekly_attacks != null ? +r.weekly_attacks : 0));
  }

  // Source-band coloring: pre-Oct 2025 = thesis Houthi maritime strikes
  // (Bab/Yemen subset, red); post-Oct 2025 = HDX Yemen + Iran combined
  // (broader scope reflecting the Hormuz war front, gold). The y-axis
  // jump at the boundary is real — coloring makes that visible at a
  // glance rather than burying it in the subtitle.
  const THESIS_END = '2025-10-01';
  const C_HOUTHI   = 'rgba(255,61,94,0.55)';   // C_RED-ish
  const C_HOUTHI_B = 'rgba(255,61,94,0.85)';
  const C_WAR      = 'rgba(255,170,0,0.55)';   // amber/gold
  const C_WAR_B    = 'rgba(255,170,0,0.85)';
  const barFills   = rows.map(r => (r.date && r.date <= THESIS_END) ? C_HOUTHI   : C_WAR);
  const barEdges   = rows.map(r => (r.date && r.date <= THESIS_END) ? C_HOUTHI_B : C_WAR_B);

  // Find the index of the first post-thesis row so we can draw a
  // dashed vertical separator + label.
  const splitIdx = rows.findIndex(r => r.date && r.date > THESIS_END);
  const splitX   = (splitIdx > 0) ? labels[splitIdx] : null;

  new Chart(canvas, {
    type: 'line',
    data: {
      labels,
      datasets: [
        { label: 'Brent (USD)', data: prices, borderColor: C_CYAN, backgroundColor: 'rgba(0,212,255,0.08)',
          borderWidth: 2, pointRadius: 0, tension: 0.35, fill: true, yAxisID: 'y', spanGaps: true },
        { label: 'Houthi maritime strikes (thesis · Bab)', data: attacks.map((v,i) => i < (splitIdx === -1 ? rows.length : splitIdx) ? v : null),
          borderColor: C_HOUTHI_B, backgroundColor: C_HOUTHI, type: 'bar', yAxisID: 'y1', barThickness: 4 },
        { label: 'Live conflict events (HDX · Yemen + Iran)', data: attacks.map((v,i) => i >= (splitIdx === -1 ? rows.length : splitIdx) ? v : null),
          borderColor: C_WAR_B, backgroundColor: C_WAR, type: 'bar', yAxisID: 'y1', barThickness: 4 }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: '#dfe7f0', boxWidth: 14, font: { size: 11 } } },
        // Vertical separator at the thesis/live data boundary. Drawn as a
        // tiny inline plugin since we don't want to pull in chartjs-annotation.
        beforeDraw: undefined,
      },
      scales: {
        x: { ticks: { color: '#556475', maxTicksLimit: 10 }, grid: { color: 'rgba(0,212,255,0.05)' } },
        y:  { position: 'left',  title: { display: true, text: 'BRENT $/BBL', color: '#556475', font: { size: 10 } },
              ticks: { color: '#8f9db0', callback: v => '$' + v }, grid: { color: 'rgba(0,212,255,0.05)' } },
        y1: { position: 'right', title: { display: true, text: 'WEEKLY EVENTS', color: '#556475', font: { size: 10 } },
              ticks: { color: '#8f9db0' }, grid: { display: false }, beginAtZero: true }
      }
    },
    plugins: [{
      id: 'thesisBoundaryLine',
      afterDatasetsDraw(chart) {
        if (!splitX) return;
        const xScale = chart.scales.x;
        const xPx    = xScale.getPixelForValue(splitX);
        if (!isFinite(xPx)) return;
        const top = chart.chartArea.top, bottom = chart.chartArea.bottom;
        const ctx = chart.ctx;
        ctx.save();
        ctx.strokeStyle = 'rgba(255,170,0,0.55)';
        ctx.lineWidth = 1.2;
        ctx.setLineDash([4, 4]);
        ctx.beginPath();
        ctx.moveTo(xPx, top);
        ctx.lineTo(xPx, bottom);
        ctx.stroke();
        ctx.setLineDash([]);
        ctx.fillStyle = '#ffaa00';
        ctx.font = '10px JetBrains Mono, monospace';
        ctx.textAlign = 'left';
        ctx.fillText('THESIS WINDOW ENDS · LIVE HDX FROM HERE', xPx + 6, top + 14);
        ctx.restore();
      }
    }]
  });
}

// ═══════════════════════════════════════════════════════════════════════════
// Realized volatility over time
// ═══════════════════════════════════════════════════════════════════════════
function renderVolatilityChart(state) {
  const canvas = chartEl('volatilityChart');
  if (!canvas) return;
  destroyChartOn(canvas);

  const ts = applyTimelineRows((state.master && state.master.timeseries) || []);
  if (!ts.length) return showChartEmpty(canvas, 'volatility unavailable');

  // Downsample to weekly
  const step = Math.max(1, Math.floor(ts.length / 60));
  const rows = [];
  for (let i = 0; i < ts.length; i += step) rows.push(ts[i]);

  const labels = rows.map(r => r.date);
  // daily_volatility is a decimal std (e.g. 0.023). Convert to % annualized roughly × √252.
  // daily_volatility is daily % points (max ~7.3 in the thesis sample).
  // Annualize by × √252. NO extra × 100 — see comment in renderScatter.
  const data = rows.map(r => r.daily_volatility != null ? +(r.daily_volatility * Math.sqrt(252)).toFixed(2) : 0);

  new Chart(canvas, {
    type: 'bar',
    data: { labels, datasets: [{
      data,
      backgroundColor: data.map(v => v > 60 ? C_RED : v > 40 ? C_AMBER : C_CYAN),
      borderWidth: 0
    }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: '#556475', maxTicksLimit: 8 }, grid: { display: false } },
        y: { title: { display: true, text: 'ANNUALIZED VOL %', color: '#556475', font: { size: 10 } },
             ticks: { color: '#8f9db0', callback: v => v + '%' }, grid: { color: 'rgba(0,212,255,0.05)' } }
      }
    }
  });
}

// ═══════════════════════════════════════════════════════════════════════════
// Event types doughnut (ACLED categories) — CLICK-TO-CROSS-FILTER
// Clicking a slice sets window.CP.filters.eventType, dims non-matching
// slices, shows a filter-active pill, and dispatches 'cross-filter-changed'
// so other listeners (KPIs, scatter) can react. Click the active slice again
// (or anywhere on the cleared pill) to release the filter.
// ═══════════════════════════════════════════════════════════════════════════
function renderEventTypesChart(state) {
  const canvas = chartEl('eventTypesChart');
  if (!canvas) return;

  // Cross-filter store (shared with other charts)
  window.CP = window.CP || {};
  window.CP.filters = window.CP.filters || {};

  // Oil-relevance filter: §03 is "Event Type Mix" — purpose is to show
  // what KIND of oil-impacting activity dominates. Protests/riots aren't
  // oil-impacting, so they're filtered out before the type-counting
  // happens. Without this the donut was dominated by Yemeni civilian
  // protests, drowning out the actual maritime / military events that
  // the chart claims to be about.
  const rawEvents = (state.events && state.events.length) ? state.events : (window.THESIS_EVENTS || []);
  const allEvents = oilRelevant(rawEvents);
  if (!allEvents.length) {
    destroyChartOn(canvas);
    window.addEventListener('events-ready',        () => renderEventTypesChart(window.CP.state), { once: true });
    window.addEventListener('thesis-events-ready', () => renderEventTypesChart(window.CP.state), { once: true });
    return showChartEmpty(canvas, 'loading events…');
  }
  // Apply temporal scrubber: only count events at or before the scrubbed date
  const tlCut = tlCutoffDate();
  const tlEvents = tlCut
    ? allEvents.filter(e => e.event_date && e.event_date <= tlCut)
    : allEvents;
  if (!tlEvents.length) {
    // Pre-crisis era: nothing to show but the chart, not the loading state
    destroyChartOn(canvas);
    return showChartEmpty(canvas, 'no events in window');
  }

  // ── Filter to the most recent 90 days of AVAILABLE data ────────────
  // Showing all 17 974 events spanning 17 months made the donut a
  // historical aggregate of the entire Houthi+war period, dominated by
  // the long quiet 2024–25 stretch. Users reading the chart wanted
  // "what's the conflict mix RIGHT NOW" — so we anchor the 90-day
  // window to the dataset's own newest event date, not wall-clock,
  // because the bundled fallback may be months stale. The subtitle
  // discloses both the window and the data-through date.
  let windowNewest = '';
  for (const e of tlEvents) {
    const d = (e.event_date || '').slice(0, 10);
    if (d && d > windowNewest) windowNewest = d;
  }
  let cutoffISO = '';
  if (windowNewest) {
    const t = new Date(windowNewest + 'T00:00:00Z').getTime();
    if (isFinite(t)) {
      cutoffISO = new Date(t - 90 * 86400000).toISOString().slice(0, 10);
    }
  }
  const events = cutoffISO
    ? tlEvents.filter(e => (e.event_date || '').slice(0, 10) >= cutoffISO)
    : tlEvents;

  const parent = canvas.parentElement;
  const overlay = parent && parent.querySelector('.chart-empty');
  if (overlay) overlay.remove();

  // Update the §03 subtitle so the user SEES the data window. ACLED's
  // free / academic API tier embargoes row-level events for ~12 months —
  // paid Premium gets real-time. We use a hybrid model by design:
  //   • §03 donut here: ACLED row-level event-type breakdown (12-mo
  //     lag, but rich category granularity)
  //   • §01 chart + chokepoint cards: HDX monthly aggregates (live,
  //     no embargo, but no per-event detail)
  // The badge wording reflects that hybrid choice rather than implying
  // anything is broken — "EMBARGOED ROW-LEVEL · LIVE COUNTS ON §01" makes
  // the design legible to a first-time viewer.
  const subtitleEl = document.querySelector('[data-event-mix-subtitle]');
  if (subtitleEl && windowNewest) {
    const m = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
    const [y, mm, d] = windowNewest.split('-').map(Number);
    const newestLabel = `${String(d).padStart(2,'0')} ${m[mm-1]} ${y}`;
    const todayMs = Date.now();
    const ageDays = Math.max(0, Math.floor((todayMs - new Date(windowNewest + 'T00:00:00Z').getTime()) / 86400000));
    let badge = '';
    if (ageDays >= 300 && ageDays <= 430) {
      // Cyan = "working as designed" (ACLED free-tier embargo, not a fault).
      badge = ` <span class="xf-stale-tag xf-info-tag" data-acled-diag="1" tabindex="0" role="button" title="ACLED free-tier API embargoes row-level events for ~12 months. Live monthly counts are on the §01 chart and chokepoint cards. Click for details.">ACLED ROW-LEVEL · 12-MO EMBARGO · LIVE COUNTS ABOVE</span>`;
    } else if (ageDays > 30) {
      // Amber = unexpected staleness (something IS wrong).
      badge = ` <span class="xf-stale-tag" data-acled-diag="1" tabindex="0" role="button" title="Click for ACLED diagnostic info">⚠ ${ageDays}d OLD · DETAILS</span>`;
    }
    subtitleEl.innerHTML =
      `Event-type breakdown from ACLED row-level data, last 90 days through <b>${newestLabel}</b>. ` +
      `<b>Filter:</b> oil-relevant types only (battles, explosions/remote violence, violence against civilians, strategic developments). Civilian protests and riots are excluded — they don't move oil markets. ` +
      `Categories shown here have a ~12-month publication delay; <b>live monthly counts</b> for the current war period are on the §01 chart and the chokepoint cards (HDX mirror, no embargo).${badge} Click a wedge to cross-filter.`;
  }

  const counts = new Map();
  for (const e of events) {
    const t = (e.event_type || e.sub_event_type || 'Other').toString();
    counts.set(t, (counts.get(t) || 0) + 1);
  }
  const entries = [...counts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 5);
  const labels = entries.map(x => x[0]);
  const data   = entries.map(x => x[1]);
  const palette = [C_RED, C_ORANGE, C_CYAN, C_PURPLE, C_GOLD];

  // Tear down prior chart instance so click handlers stay clean across re-renders
  const prior = (typeof Chart.getChart === 'function') ? Chart.getChart(canvas) : null;
  if (prior) prior.destroy();

  // Build/refresh the floating "FILTER: <type>" pill
  if (parent && getComputedStyle(parent).position === 'static') parent.style.position = 'relative';
  let pill = parent && parent.querySelector('.xf-pill');
  if (!pill && parent) {
    pill = document.createElement('div');
    pill.className = 'xf-pill';
    pill.style.cssText = 'position:absolute;top:8px;left:8px;padding:4px 8px;font:10px "JetBrains Mono",monospace;letter-spacing:1.5px;border-radius:3px;cursor:pointer;display:none;background:rgba(0,212,255,0.15);color:#dfe7f0;border:1px solid rgba(0,212,255,0.45);z-index:5';
    parent.appendChild(pill);
  }

  function applyFilterVisual(sel) {
    const colors = labels.map((lbl, i) => {
      const base = palette[i];
      if (!sel) return base;
      return lbl === sel ? base : (base + '33'); // dim non-selected (~20% alpha)
    });
    chart.data.datasets[0].backgroundColor = colors;
    chart.update('none');
    if (pill) {
      if (sel) {
        pill.textContent = `FILTER: ${sel.toUpperCase()} · CLEAR ✕`;
        pill.style.display = 'block';
      } else {
        pill.style.display = 'none';
      }
    }
  }

  const chart = new Chart(canvas, {
    type: 'doughnut',
    data: { labels, datasets: [{
      data,
      backgroundColor: palette.slice(0, labels.length),
      borderColor: '#0b1119', borderWidth: 2,
      hoverOffset: 6
    }] },
    options: {
      responsive: true, maintainAspectRatio: false, cutout: '62%',
      plugins: {
        legend: { position: 'right', labels: { color: '#dfe7f0', boxWidth: 10, padding: 8, font: { size: 11 } } },
        tooltip: {
          callbacks: {
            afterLabel: () => '· click to filter'
          }
        }
      },
      onClick: (_evt, elements) => {
        if (!elements || !elements.length) return;
        const idx = elements[0].index;
        const next = labels[idx];
        const prev = window.CP.filters.eventType;
        window.CP.filters.eventType = (prev === next) ? null : next;
        applyFilterVisual(window.CP.filters.eventType);
        window.dispatchEvent(new CustomEvent('cross-filter-changed', {
          detail: { eventType: window.CP.filters.eventType }
        }));
      }
    }
  });

  // Pill also acts as a clear-button
  if (pill) {
    pill.onclick = () => {
      if (!window.CP.filters.eventType) return;
      window.CP.filters.eventType = null;
      applyFilterVisual(null);
      window.dispatchEvent(new CustomEvent('cross-filter-changed', {
        detail: { eventType: null }
      }));
    };
  }

  // Re-apply existing filter on re-render
  applyFilterVisual(window.CP.filters.eventType || null);
}

// ── Cross-filter / timeline consumer: update the incidents-30d KPI to show
// the count of events in the 30-day window ending at the scrubbed date,
// optionally narrowed to the cross-filter event type. Falls back to the
// chokepoint-aggregated count maintained by hydrate.js when no filters
// are active and the scrubber sits at "now".
function updateIncidentsKpi() {
  const incidentEl = document.querySelector('[data-kpi="incidents30"]');
  if (!incidentEl) return;
  const deltaEl = document.querySelector('[data-kpi="incidents30Delta"]');
  const sel = window.CP && window.CP.filters && window.CP.filters.eventType;
  const tlTs = tlCutoffTs();
  // Hero "INCIDENTS · 30D" counts oil-impacting events only — strip
  // protests/riots from the dataset before counting. This matches the
  // server-side chokepoint_incidents filter and the chokepoint card
  // sub-label "(THESIS WINDOW · excl. protests)".
  const events = oilRelevant(
    (window.CP && window.CP.state && window.CP.state.events) || window.THESIS_EVENTS || []
  );

  // Compute current 30-day count and prior 30-day count for the delta display.
  // Even when scrubber is at "now" we want the delta vs the prior 30 days, so
  // count from events directly rather than deferring to the chokepoint sum.
  // When no scrubber cut is active, anchor the window to the dataset's most
  // recent event (the ACLED fallback is frozen post-thesis, so `Date.now()`
  // would collapse the 30-day window to 0 events).
  const anchorNow = (typeof window.__dataNowTs === 'function')
    ? window.__dataNowTs(events)
    : Date.now();
  const endTs = tlTs || anchorNow;
  const startTs = endTs - 30 * 24 * 3600 * 1000;
  const priorEnd = startTs;
  const priorStart = priorEnd - 30 * 24 * 3600 * 1000;
  let n = 0, prior = 0;
  for (const ev of events) {
    if (sel) {
      const t = (ev.event_type || ev.sub_event_type || 'Other').toString();
      if (t !== sel) continue;
    }
    const dt = ev.event_date ? Date.parse(ev.event_date) : NaN;
    if (isNaN(dt)) continue;
    if (dt >= startTs && dt <= endTs)        n++;
    else if (dt >= priorStart && dt < priorEnd) prior++;
  }

  // No filters, scrubber at "now", and events not yet loaded: defer to the
  // chokepoint-aggregated count so we don't show 0 while events load.
  if (!sel && !tlTs && !events.length) {
    const cps = window.CHOKEPOINTS || {};
    n = ((cps.hormuz && cps.hormuz.incidents30d) || 0)
      + ((cps.bab    && cps.bab.incidents30d)    || 0);
  }
  incidentEl.textContent = String(n);

  // Delta: signed change vs prior 30D
  if (deltaEl) {
    const d = n - prior;
    const sign = d > 0 ? '+' : d < 0 ? '−' : '±';
    deltaEl.textContent = `${sign}${Math.abs(d)} vs prior 30D`;
    deltaEl.classList.toggle('up-alert', d > 0);
    deltaEl.classList.toggle('down', d < 0);
  }

  // Foot: make the anchor date explicit so a frozen fallback dataset doesn't
  // look like a live feed gone silent. Only overwrite once we actually have
  // events and we're anchored to the dataset (not a live real-time window).
  const footEl = document.querySelector('[data-kpi-foot="incidents30"]');
  if (footEl && events.length) {
    const anchorDate = new Date(endTs);
    const y = anchorDate.getUTCFullYear();
    const m = String(anchorDate.getUTCMonth() + 1).padStart(2, '0');
    const d = String(anchorDate.getUTCDate()).padStart(2, '0');
    footEl.textContent = `ACLED · 30D ending ${y}-${m}-${d}`;
  }
}

(function wireCrossFilterIncidentsKpi() {
  if (window.__xfIncidentsWired) return;
  window.__xfIncidentsWired = true;
  window.addEventListener('cross-filter-changed', updateIncidentsKpi);
  window.addEventListener('timeline-set',         updateIncidentsKpi);
  // Refresh once events arrive in the background
  window.addEventListener('events-ready',         updateIncidentsKpi);
  window.addEventListener('thesis-events-ready',  updateIncidentsKpi);
})();

// ── Timeline-driven re-render of every chart that consumes timeseries or
// dated events. Each render function destroys its prior Chart instance,
// applies the cutoff, and rebuilds. Coalesce repeated scrubbing into one
// re-render per animation frame so dragging stays smooth. Suppressed until
// after the initial hydrated render has populated window.CP.state — the
// initial 'timeline-set' fires before hydration, and re-rendering with an
// empty state would paint "data unavailable" overlays that stick.
(function wireTimelineRerender() {
  if (window.__tlRerenderWired) return;
  window.__tlRerenderWired = true;
  let pending = false;
  function isStateReady() {
    const s = window.CP && window.CP.state;
    return !!(s && (s.master || (s.brent && s.brent.length)));
  }
  function flush() {
    pending = false;
    if (!isStateReady()) return; // skip — initial paint will pick up the timeline anyway
    const state = window.CP.state;
    try { renderPriceVsAttacks(state); } catch (e) {}
    try { renderVolatilityChart(state); } catch (e) {}
    try { renderEventTypesChart(state); } catch (e) {}
    try { renderScatter(state); } catch (e) {}
    try { renderDxyOvx(state); } catch (e) {}
    try { renderIranTimeline(state); } catch (e) {}
  }
  window.addEventListener('timeline-set', () => {
    if (pending) return;
    pending = true;
    requestAnimationFrame(flush);
  });
})();

// ── Cross-filter re-render: when the doughnut (or its CLEAR pill) toggles
// window.CP.filters.eventType, re-paint the consumers that change appearance
// based on the active filter. The doughnut itself updates inline; this hook
// catches every other chart that needs a banner or marker dim.
(function wireCrossFilterRerender() {
  if (window.__xfRerenderWired) return;
  window.__xfRerenderWired = true;
  let pending = false;
  function isStateReady() {
    const s = window.CP && window.CP.state;
    return !!(s && (s.master || (s.brent && s.brent.length)));
  }
  function flush() {
    pending = false;
    if (!isStateReady()) return;
    const state = window.CP.state;
    try { renderPriceVsAttacks(state); } catch (e) {}
    try { renderScatter(state); }        catch (e) {}
    try { renderIranTimeline(state); }   catch (e) {}
  }
  window.addEventListener('cross-filter-changed', () => {
    if (pending) return;
    pending = true;
    requestAnimationFrame(flush);
  });
  // When heavy events arrive AFTER a filter is already active, re-run the
  // price chart so its weekly bars recompute from the now-populated event
  // list (init-time render saw an empty events array).
  window.addEventListener('events-ready', () => {
    if (!xfActiveType()) return;
    if (pending) return;
    pending = true;
    requestAnimationFrame(flush);
  });
})();

// ═══════════════════════════════════════════════════════════════════════════
// Price window (T-2 … T+5 around events) — from master.price_windows
// ═══════════════════════════════════════════════════════════════════════════
function renderPriceWindow(state) {
  const canvas = chartEl('priceWindowChart');
  if (!canvas) return;
  destroyChartOn(canvas);

  const pw = (state.master && state.master.price_windows) || {};
  const order = ['Price_T-2','Price_T-1','Price_T0','Price_T+1','Price_T+2','Price_T+3','Price_T+4','Price_T+5'];
  const data = order.map(k => pw[k] != null ? +pw[k] : null);
  if (data.every(v => v == null)) return showChartEmpty(canvas, 'event window unavailable');

  const labels = order.map(k => k.replace('Price_', ''));
  const valid = data.filter(v => v != null);
  const minY = Math.max(0, Math.min(...valid) - 2);

  new Chart(canvas, {
    type: 'bar',
    data: { labels, datasets: [{
      data, spanGaps: false,
      backgroundColor: (ctx) => ctx.dataIndex === 2 ? C_RED : 'rgba(0,212,255,0.5)',
      borderRadius: 2
    }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: '#8f9db0' }, grid: { display: false } },
        y: { ticks: { color: '#8f9db0', callback: v => '$' + v.toFixed(2) }, grid: { color: 'rgba(0,212,255,0.05)' }, beginAtZero: false, min: minY }
      }
    }
  });
}

// ═══════════════════════════════════════════════════════════════════════════
// Scatter: Weekly attacks vs volatility
// ═══════════════════════════════════════════════════════════════════════════
function renderScatter(state) {
  // Two regimes with very different unit scales:
  //   • THESIS WINDOW (Oct 2023 – Sep 2025): WeeklyAttackFreq is a curated
  //     count of HOUTHI MARITIME strikes. x usually 0–47.
  //   • LIVE EXTENSION (Oct 2025 – today):  weekly_attacks is HDX Yemen + Iran
  //     political-violence events (no protests). x usually 50–250.
  //
  // Plotting them on a shared x-axis collapses the thesis cluster into the
  // left ~20% of the chart and the live cluster into the right ~80% with
  // empty middle space — unreadable.
  //
  // Solution: side-by-side small multiples. Each regime gets its own
  // canvas with its own axis scale. The user immediately sees the within-
  // regime relationship (slope of OLS fit) and the regime shift between
  // panels without any scale-distortion artifacts.
  const thesisCanvas = chartEl('scatterChartThesis');
  const liveCanvas   = chartEl('scatterChartLive');
  if (!thesisCanvas && !liveCanvas) return;
  if (thesisCanvas) destroyChartOn(thesisCanvas);
  if (liveCanvas)   destroyChartOn(liveCanvas);

  const ts = applyTimelineRows((state.master && state.master.timeseries) || []);
  if (!ts.length) {
    if (thesisCanvas) showChartEmpty(thesisCanvas, 'scatter unavailable');
    if (liveCanvas)   showChartEmpty(liveCanvas,   'scatter unavailable');
    return;
  }

  const THESIS_END = '2025-10-01';
  const thesisPts = [];
  const livePts = [];
  // master.timeseries.daily_volatility is stored as DAILY % POINTS
  // (e.g. 7.266 means 7.27% intraday move), already pre-multiplied by
  // 100. To annualize: multiply by √252. Do NOT multiply by 100 again
  // — that's what was producing the 12,000% / 4,500% absurd y-axes.
  for (const r of ts) {
    if (r.weekly_attacks == null || r.daily_volatility == null) continue;
    const x = +r.weekly_attacks;
    const y = +r.daily_volatility * Math.sqrt(252);
    if (!isFinite(x) || !isFinite(y)) continue;
    const pt = { x, y };
    if (r.date && r.date <= THESIS_END) thesisPts.push(pt);
    else                                  livePts.push(pt);
  }

  // OLS fit helper. Returns slope, intercept, R², and the line segment
  // covering the actual x-range of the data so the dashed trendline
  // doesn't extrapolate into empty space.
  function olsSegment(pts) {
    if (pts.length < 3) return null;
    const n = pts.length;
    let sx = 0, sy = 0, sxy = 0, sxx = 0;
    for (const p of pts) { sx += p.x; sy += p.y; sxy += p.x * p.y; sxx += p.x * p.x; }
    const mx = sx / n, my = sy / n;
    const denom = sxx - n * mx * mx;
    if (denom <= 0) return null;
    const slope = (sxy - n * mx * my) / denom;
    const intercept = my - slope * mx;
    const xs = pts.map(p => p.x);
    const xMin = Math.min(...xs), xMax = Math.max(...xs);
    let ssTot = 0, ssRes = 0;
    for (const p of pts) {
      ssTot += (p.y - my) ** 2;
      const yhat = intercept + slope * p.x;
      ssRes += (p.y - yhat) ** 2;
    }
    return {
      seg: [{ x: xMin, y: intercept + slope * xMin }, { x: xMax, y: intercept + slope * xMax }],
      slope,
      r2: ssTot > 0 ? 1 - ssRes / ssTot : null,
      n,
    };
  }

  function renderPanel(canvas, pts, color, regimeLabel) {
    if (!canvas) return;
    if (!pts.length) {
      showChartEmpty(canvas, regimeLabel + ' · no data in window');
      return;
    }
    const fit = olsSegment(pts);
    const datasets = [
      {
        label: `${regimeLabel} (n=${pts.length})`,
        type: 'scatter', data: pts,
        backgroundColor: color + '88',
        borderColor: color,
        pointRadius: 3.5, pointHoverRadius: 6,
      },
    ];
    if (fit) {
      datasets.push({
        label: `OLS · β=${fit.slope.toFixed(3)} · R²=${(fit.r2 || 0).toFixed(2)}`,
        type: 'line', data: fit.seg,
        borderColor: color, borderWidth: 1.5, borderDash: [5, 4],
        pointRadius: 0, fill: false, tension: 0,
      });
    }
    new Chart(canvas, {
      data: { datasets },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { labels: { color: '#dfe7f0', boxWidth: 10, font: { size: 9 }, padding: 6 } },
          tooltip: {
            callbacks: {
              label: (ctx) => {
                if (ctx.dataset.type !== 'scatter') return ctx.dataset.label;
                return `${ctx.parsed.x.toFixed(0)} events → ${ctx.parsed.y.toFixed(1)}% ann. vol`;
              },
            },
          },
        },
        scales: {
          x: { type: 'linear',
               title: { display: true, text: 'WEEKLY EVENTS', color: '#556475', font: { size: 9 } },
               ticks: { color: '#8f9db0', font: { size: 9 } },
               grid: { color: 'rgba(0,212,255,0.05)' } },
          y: { title: { display: true, text: 'ANNUALIZED VOL (%)', color: '#556475', font: { size: 9 } },
               ticks: { color: '#8f9db0', font: { size: 9 }, callback: v => v + '%' },
               grid: { color: 'rgba(0,212,255,0.05)' } },
        },
      },
    });
  }

  renderPanel(thesisCanvas, thesisPts, '#ff5252', 'Thesis · Houthi');
  renderPanel(liveCanvas,   livePts,   '#ffaa00', 'Live · HDX Yemen+Iran');
}

// ═══════════════════════════════════════════════════════════════════════════
// DXY vs OVX
// ═══════════════════════════════════════════════════════════════════════════
function renderDxyOvx(state) {
  const canvas = chartEl('dxyOvxChart');
  if (!canvas) return;
  destroyChartOn(canvas);

  // Pull from master timeseries (daily)
  const ts = applyTimelineRows((state.master && state.master.timeseries) || []);
  if (!ts.length) return showChartEmpty(canvas, 'DXY/OVX unavailable');

  const step = Math.max(1, Math.floor(ts.length / 60));
  const rows = [];
  for (let i = 0; i < ts.length; i += step) rows.push(ts[i]);

  const labels   = rows.map(r => r.date);
  const dxyData  = rows.map(r => r.dxy != null ? +r.dxy : null);
  const ovxData  = rows.map(r => r.ovx != null ? +r.ovx : null);

  new Chart(canvas, {
    type: 'line',
    data: { labels, datasets: [
      { label: 'DXY', data: dxyData, borderColor: C_PURPLE, backgroundColor: 'rgba(155,123,238,0.08)', borderWidth: 2, pointRadius: 0, fill: true, yAxisID: 'y', tension: 0.35, spanGaps: true },
      { label: 'OVX', data: ovxData, borderColor: C_ORANGE, borderWidth: 2, pointRadius: 0, yAxisID: 'y1', tension: 0.35, spanGaps: true }
    ] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { labels: { color: '#dfe7f0' } } },
      scales: {
        x: { ticks: { color: '#556475', maxTicksLimit: 6 }, grid: { color: 'rgba(0,212,255,0.05)' } },
        y:  { position: 'left',  title: { display: true, text: 'DXY', color: '#556475', font: { size: 10 } },
              ticks: { color: '#9b7bee' }, grid: { color: 'rgba(0,212,255,0.05)' } },
        y1: { position: 'right', title: { display: true, text: 'OVX', color: '#556475', font: { size: 10 } },
              ticks: { color: '#ff8c42' }, grid: { display: false } }
      }
    }
  });
}

// ═══════════════════════════════════════════════════════════════════════════
// Correlation matrix
// ═══════════════════════════════════════════════════════════════════════════
function renderCorrelation(state) {
  const container = chartEl('corrMatrix');
  if (!container) return;

  const corr = (state.master && state.master.correlation) || null;
  if (!corr || !corr.labels || !corr.matrix) {
    container.innerHTML = '<div style="padding:40px;text-align:center;color:#556475;font:11px JetBrains Mono,monospace;letter-spacing:2px">CORRELATION MATRIX UNAVAILABLE</div>';
    return;
  }

  // Short-label lookup for readability
  const shortLabel = s => ({
    'Brent_Price': 'BRENT', 'Daily_Volatility': 'VOL', 'WeeklyAttackFreq': 'ATTACKS',
    'DXY': 'DXY', 'OVX': 'OVX', 'OPEC_Dummy': 'OPEC', 'RussiaUkraine_Dummy': 'RUS-UKR',
    'IranIsrael_Escalation': 'IR-IL', 'China_PMI': 'CHN PMI', 'Baker_Hughes_Rigs': 'RIGS',
    'SPR_Release_Volume': 'SPR'
  }[s] || s.slice(0, 7).toUpperCase());

  const vars = corr.labels.map(shortLabel);
  const m = corr.matrix;

  // Defensive: bail if matrix isn't square against labels
  if (!Array.isArray(m) || m.length !== vars.length || m.some(r => !Array.isArray(r) || r.length !== vars.length)) {
    container.innerHTML = '<div style="padding:40px;text-align:center;color:#556475;font:11px JetBrains Mono,monospace;letter-spacing:2px">CORRELATION MATRIX MALFORMED</div>';
    return;
  }

  container.innerHTML = '';
  const cols = `80px repeat(${vars.length}, 1fr)`;
  container.style.gridTemplateColumns = cols;

  const mkLabel = t => { const d = document.createElement('div'); d.className = 'corr-label'; d.textContent = t; return d; };

  const head = document.createElement('div'); head.className = 'corr-row'; head.style.gridTemplateColumns = cols;
  head.appendChild(mkLabel(''));
  vars.forEach(v => head.appendChild(mkLabel(v)));
  container.appendChild(head);

  m.forEach((row, i) => {
    const r = document.createElement('div'); r.className = 'corr-row'; r.style.gridTemplateColumns = cols;
    r.appendChild(mkLabel(vars[i]));
    row.forEach((v, j) => {
      const c = document.createElement('div'); c.className = 'corr-cell';
      const abs = Math.abs(v);
      if (i === j) {
        c.style.background = 'rgba(0,212,255,0.85)';
        c.style.color = '#03111a';
        c.style.fontWeight = '700';
      } else {
        c.style.background = v >= 0
          ? `rgba(0,212,255,${0.08 + abs * 0.65})`
          : `rgba(255,61,94,${0.08 + abs * 0.65})`;
        c.style.color = abs > 0.4 ? '#fff' : '#dfe7f0';
      }
      c.textContent = v.toFixed(2);
      r.appendChild(c);
    });
    container.appendChild(r);
  });
}

// ═══════════════════════════════════════════════════════════════════════════
// Iran timeline — curated events on Brent price line
// ═══════════════════════════════════════════════════════════════════════════
function renderIranTimeline(state) {
  const canvas = chartEl('iranTimelineChart');
  if (!canvas) return;
  destroyChartOn(canvas);

  const brent = state.brent || [];
  const tlCut = tlCutoffDate();
  const events = (window.IRAN_EVENTS || []).filter(e => !tlCut || (e.date && e.date <= tlCut));
  if (!brent.length) return showChartEmpty(canvas, 'Brent prices unavailable');

  // Filter Brent to Oct 2025 → (scrubbed cutoff or latest)
  const filtered = brent.filter(r => r.date >= '2025-10-01' && (!tlCut || r.date <= tlCut));
  if (!filtered.length) {
    // Pre-Oct-2025 scrub: war hasn't started yet on the timeline
    return showChartEmpty(canvas, 'no Brent data in window · scrub forward to Oct 2025+');
  }
  const labels = filtered.map(r => r.date);
  const prices = filtered.map(r => r.price);

  // Build a sorted Brent date→price map so we can join each curated event to
  // the nearest trading-day price (events may fall on weekends/holidays, or
  // the backend's curated list may omit brent_price entirely — both cases
  // used to make markers silently disappear).
  const brentByDate = new Map();
  for (const r of filtered) brentByDate.set(r.date, +r.price);
  const brentDates = labels; // already sorted ascending
  function nearestBrent(dateStr) {
    if (brentByDate.has(dateStr)) return { date: dateStr, price: brentByDate.get(dateStr) };
    // Binary search for the most recent trading day on or before the event.
    let lo = 0, hi = brentDates.length - 1, best = -1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      if (brentDates[mid] <= dateStr) { best = mid; lo = mid + 1; } else { hi = mid - 1; }
    }
    if (best === -1) return null;  // event precedes Brent window
    const d = brentDates[best];
    return { date: d, price: brentByDate.get(d) };
  }

  const eventColor = t =>
    t === 'military'    ? C_RED :
    t === 'nuclear'     ? C_PURPLE :
    t === 'sanctions'   ? C_ORANGE :
    t === 'proxy'       ? C_GOLD :
                          C_BLUE;

  // Cross-filter: dim event markers whose Iran-domain type doesn't map to the
  // currently selected ACLED filter type from the doughnut.
  const xf = xfActiveType();
  showXfBanner(canvas, xf);
  const eventDatasets = events.map(e => {
    // Prefer the event's own price if the backend supplied one; otherwise
    // snap to the nearest Brent trading day in the chart's window.
    let x = e.date, y = (e.price != null ? +e.price : null);
    if (y == null || !brentByDate.has(e.date)) {
      const row = nearestBrent(e.date);
      if (!row) return null;          // event before Brent window — skip
      x = row.date;                   // snap to an existing category label
      if (y == null) y = row.price;
    }
    const match = xfMatchesIranType(xf, e.type);
    // Visual distinction between curated (solid filled circle) and live
    // news (hollow ring with the type's color as the stroke). Live
    // markers are smaller so the curated war-period events still
    // dominate the eye but post-cutoff continuation is clearly visible.
    const isLive = e.source === 'live-news';
    const color = eventColor(e.type);
    return {
      label: e.label + (isLive ? ' · LIVE' : ''),
      type: 'scatter',
      data: [{ x, y }],
      backgroundColor: isLive
        ? 'rgba(0,0,0,0)'                                                // hollow for live
        : (match ? color : color + '30'),                                // solid for curated
      borderColor: isLive
        ? (match ? color : color + '60')                                 // colored ring
        : (match ? '#fff' : 'rgba(255,255,255,0.2)'),
      borderWidth: isLive ? 2 : 1.5,
      pointRadius:      isLive ? (match ? 5 : 3) : (match ? 7 : 4),
      pointHoverRadius: isLive ? (match ? 8 : 4) : (match ? 10 : 5),
      pointStyle: 'circle',
      order: 0,
      // Carry source through so the tooltip can show "LIVE NEWS" vs "CURATED"
      _evSource: e.source,
      _evUrl: e.url || null,
    };
  }).filter(Boolean);

  new Chart(canvas, {
    type: 'line',
    data: {
      labels,
      datasets: [
        { label: 'Brent (USD)', data: prices, borderColor: C_CYAN, backgroundColor: 'rgba(0,212,255,0.10)',
          borderWidth: 2, pointRadius: 0, tension: 0.25, fill: true, order: 10 },
        ...eventDatasets
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'nearest', intersect: true },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            // For scatter event markers, dataset.label IS the event title;
            // for the Brent line the X-axis label IS the date — use the right one.
            title: items => {
              const it = items[0];
              if (it && it.dataset && it.dataset.type === 'scatter') return it.dataset.label;
              return it ? it.label : '';
            },
            label: ctx => {
              if (ctx.dataset.type === 'scatter') {
                const src = ctx.dataset._evSource === 'live-news'
                  ? '· LIVE NEWS' : '· CURATED';
                return `${ctx.parsed.x} — $${ctx.parsed.y} ${src}`;
              }
              return `Brent  $${ctx.parsed.y}`;
            }
          }
        }
      },
      scales: {
        x: {
          ticks: {
            color: '#556475', maxTicksLimit: 8,
            // Parse the ISO label directly — `new Date('2025-10-01')` parses
            // as UTC midnight and local TZ shifts to the prior day/month, so
            // the first tick rendered as "Sep 25" instead of "Oct 25".
            callback: function (v) {
              const lbl = this.getLabelForValue(v);
              const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(lbl));
              if (!m) return lbl;
              const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
              return `${months[parseInt(m[2], 10) - 1]} ${parseInt(m[3], 10)}`;
            }
          },
          grid: { color: 'rgba(0,212,255,0.05)' }
        },
        y: {
          ticks: { color: '#8f9db0', callback: v => '$' + v },
          grid: { color: 'rgba(0,212,255,0.05)' },
          suggestedMin: 65, suggestedMax: 125
        }
      }
    }
  });
}
