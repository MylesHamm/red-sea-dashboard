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

  const ts = (state.master && state.master.timeseries) || [];
  if (!ts.length) return showChartEmpty(canvas, 'timeseries unavailable');

  // Weekly: sample every ~7 days from master timeseries
  const step = Math.max(1, Math.floor(ts.length / 78)); // ~78 weeks max
  const rows = [];
  for (let i = 0; i < ts.length; i += step) rows.push(ts[i]);
  // Keep the most recent point regardless
  if (rows[rows.length - 1] !== ts[ts.length - 1]) rows.push(ts[ts.length - 1]);

  const labels   = rows.map(r => r.date);
  const prices   = rows.map(r => (r.brent_price != null ? +r.brent_price : null));
  const attacks  = rows.map(r => (r.weekly_attacks != null ? +r.weekly_attacks : 0));

  new Chart(canvas, {
    type: 'line',
    data: {
      labels,
      datasets: [
        { label: 'Brent (USD)', data: prices, borderColor: C_CYAN, backgroundColor: 'rgba(0,212,255,0.08)',
          borderWidth: 2, pointRadius: 0, tension: 0.35, fill: true, yAxisID: 'y', spanGaps: true },
        { label: 'Weekly attacks', data: attacks, borderColor: C_RED, backgroundColor: 'rgba(255,61,94,0.35)',
          type: 'bar', yAxisID: 'y1', barThickness: 4 }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { labels: { color: '#dfe7f0' } } },
      scales: {
        x: { ticks: { color: '#556475', maxTicksLimit: 10 }, grid: { color: 'rgba(0,212,255,0.05)' } },
        y:  { position: 'left',  ticks: { color: '#8f9db0', callback: v => '$' + v }, grid: { color: 'rgba(0,212,255,0.05)' } },
        y1: { position: 'right', ticks: { color: '#ff3d5e' }, grid: { display: false }, beginAtZero: true }
      }
    }
  });
}

// ═══════════════════════════════════════════════════════════════════════════
// Realized volatility over time
// ═══════════════════════════════════════════════════════════════════════════
function renderVolatilityChart(state) {
  const canvas = chartEl('volatilityChart');
  if (!canvas) return;

  const ts = (state.master && state.master.timeseries) || [];
  if (!ts.length) return showChartEmpty(canvas, 'volatility unavailable');

  // Downsample to weekly
  const step = Math.max(1, Math.floor(ts.length / 60));
  const rows = [];
  for (let i = 0; i < ts.length; i += step) rows.push(ts[i]);

  const labels = rows.map(r => r.date);
  // daily_volatility is a decimal std (e.g. 0.023). Convert to % annualized roughly × √252.
  const data = rows.map(r => r.daily_volatility != null ? +(r.daily_volatility * Math.sqrt(252) * 100).toFixed(2) : 0);

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
        y: { ticks: { color: '#8f9db0', callback: v => v + '%' }, grid: { color: 'rgba(0,212,255,0.05)' } }
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

  const events = (state.events && state.events.length) ? state.events : (window.THESIS_EVENTS || []);
  if (!events.length) {
    window.addEventListener('events-ready',        () => renderEventTypesChart(window.CP.state), { once: true });
    window.addEventListener('thesis-events-ready', () => renderEventTypesChart(window.CP.state), { once: true });
    return showChartEmpty(canvas, 'loading events…');
  }
  const parent = canvas.parentElement;
  const overlay = parent && parent.querySelector('.chart-empty');
  if (overlay) overlay.remove();

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

// ── Cross-filter consumer: update the incidents-30d KPI to show the
// filtered event count when an event type is selected. Restores the
// unfiltered chokepoint-aggregated count when the filter clears.
(function wireCrossFilterIncidentsKpi() {
  if (window.__xfIncidentsWired) return;
  window.__xfIncidentsWired = true;
  window.addEventListener('cross-filter-changed', (e) => {
    const sel = e.detail && e.detail.eventType;
    const incidentEl = document.querySelector('[data-kpi="incidents30"]');
    if (!incidentEl) return;
    if (!sel) {
      // Restore the chokepoint-based count maintained by hydrate.js
      const cps = window.CHOKEPOINTS || {};
      const n = ((cps.hormuz && cps.hormuz.incidents30d) || 0)
              + ((cps.bab    && cps.bab.incidents30d)    || 0);
      incidentEl.textContent = String(n);
      return;
    }
    const events = (window.CP && window.CP.state && window.CP.state.events) || window.THESIS_EVENTS || [];
    const cutoff = Date.now() - 30 * 24 * 3600 * 1000;
    let n = 0;
    for (const ev of events) {
      const t = (ev.event_type || ev.sub_event_type || 'Other').toString();
      if (t !== sel) continue;
      const dt = ev.event_date ? Date.parse(ev.event_date) : NaN;
      if (!isNaN(dt) && dt >= cutoff) n++;
    }
    incidentEl.textContent = String(n);
  });
})();

// ═══════════════════════════════════════════════════════════════════════════
// Price window (T-2 … T+5 around events) — from master.price_windows
// ═══════════════════════════════════════════════════════════════════════════
function renderPriceWindow(state) {
  const canvas = chartEl('priceWindowChart');
  if (!canvas) return;

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
  const canvas = chartEl('scatterChart');
  if (!canvas) return;

  const ts = (state.master && state.master.timeseries) || [];
  if (!ts.length) return showChartEmpty(canvas, 'scatter unavailable');

  const pts = ts
    .filter(r => r.weekly_attacks != null && r.daily_volatility != null)
    .map(r => ({ x: +r.weekly_attacks, y: +r.daily_volatility * Math.sqrt(252) * 100 }));
  if (!pts.length) return showChartEmpty(canvas, 'scatter unavailable');

  new Chart(canvas, {
    type: 'scatter',
    data: { datasets: [{ data: pts, backgroundColor: 'rgba(0,212,255,0.55)', pointRadius: 3.5, borderColor: C_CYAN }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { title: { display: true, text: 'WEEKLY ATTACKS', color: '#556475' }, ticks: { color: '#8f9db0' }, grid: { color: 'rgba(0,212,255,0.05)' } },
        y: { title: { display: true, text: 'ANNUALIZED VOL %', color: '#556475' }, ticks: { color: '#8f9db0', callback: v => v + '%' }, grid: { color: 'rgba(0,212,255,0.05)' } }
      }
    }
  });
}

// ═══════════════════════════════════════════════════════════════════════════
// DXY vs OVX
// ═══════════════════════════════════════════════════════════════════════════
function renderDxyOvx(state) {
  const canvas = chartEl('dxyOvxChart');
  if (!canvas) return;

  // Pull from master timeseries (daily)
  const ts = (state.master && state.master.timeseries) || [];
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
        y:  { position: 'left',  ticks: { color: '#9b7bee' }, grid: { color: 'rgba(0,212,255,0.05)' } },
        y1: { position: 'right', ticks: { color: '#ff8c42' }, grid: { display: false } }
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

  const brent = state.brent || [];
  const events = window.IRAN_EVENTS || [];
  if (!brent.length) return showChartEmpty(canvas, 'Brent prices unavailable');

  // Filter Brent to Oct 2025 → latest
  const filtered = brent.filter(r => r.date >= '2025-10-01');
  const labels = filtered.map(r => r.date);
  const prices = filtered.map(r => r.price);

  const eventColor = t =>
    t === 'military'    ? C_RED :
    t === 'nuclear'     ? C_PURPLE :
    t === 'sanctions'   ? C_ORANGE :
    t === 'proxy'       ? C_GOLD :
                          C_BLUE;

  const eventDatasets = events.map(e => ({
    label: e.label,
    type: 'scatter',
    data: [{ x: e.date, y: +e.price }],
    backgroundColor: eventColor(e.type),
    borderColor: '#fff', borderWidth: 1.5,
    pointRadius: 7, pointHoverRadius: 10, pointStyle: 'circle',
    order: 0
  }));

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
            title: items => items[0].label,
            label: ctx => ctx.dataset.type === 'scatter'
              ? `${ctx.dataset.label} — $${ctx.parsed.y}`
              : `Brent  $${ctx.parsed.y}`
          }
        }
      },
      scales: {
        x: {
          ticks: {
            color: '#556475', maxTicksLimit: 8,
            callback: function (v) {
              const lbl = this.getLabelForValue(v);
              const d = new Date(lbl);
              if (isNaN(d)) return lbl;
              const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
              return `${months[d.getMonth()]} ${String(d.getFullYear()).slice(2)}`;
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
