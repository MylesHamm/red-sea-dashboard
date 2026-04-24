/* App shell — tabs, sparkline, chokepoint panel wiring, data explorer */

document.addEventListener('DOMContentLoaded', () => {

  // ── Tabs ──
  const navItems = document.querySelectorAll('.nav-item');
  const tabs = document.querySelectorAll('.tab-content');
  navItems.forEach(btn => {
    btn.addEventListener('click', () => {
      const name = btn.dataset.tab;
      navItems.forEach(b => b.classList.toggle('active', b === btn));
      tabs.forEach(t => t.classList.toggle('active', t.id === `tab-${name}`));
      // persist
      try { localStorage.setItem('cp_tab', name); } catch(e){}
      window.scrollTo({ top: 0, behavior: 'smooth' });
    });
  });
  // restore tab
  try {
    const saved = localStorage.getItem('cp_tab');
    if (saved) {
      const btn = document.querySelector(`.nav-item[data-tab="${saved}"]`);
      if (btn) btn.click();
    }
  } catch(e){}

  // ── Sparkline (hero KPI) ──
  const sparkLine = document.getElementById('sparkLine');
  const sparkPath = document.getElementById('sparkPath');
  if (sparkLine && window.BRENT_SPARK) {
    const data = window.BRENT_SPARK;
    const w = 200, h = 50, pad = 2;
    const min = Math.min(...data), max = Math.max(...data);
    const x = i => pad + (i/(data.length-1)) * (w - pad*2);
    const y = v => h - pad - ((v-min)/(max-min)) * (h - pad*2);
    const line = data.map((v,i) => `${i===0?'M':'L'}${x(i)},${y(v)}`).join(' ');
    const fill = line + ` L${x(data.length-1)},${h} L${x(0)},${h} Z`;
    sparkLine.setAttribute('d', line);
    sparkPath.setAttribute('d', fill);
  }

  // ── Chokepoint panel wiring ──
  const gpEls = {
    title: document.getElementById('gpTitle'),
    coords: document.getElementById('gpCoords'),
    fill: document.getElementById('gpThreatFill'),
    tag: document.getElementById('gpThreatTag'),
    flow: document.getElementById('gpFlow'),
    width: document.getElementById('gpWidth'),
    vessels: document.getElementById('gpVessels'),
    incidents: document.getElementById('gpIncidents'),
    routes: document.getElementById('gpRoutes'),
    vesselList: document.getElementById('gpVesselList')
  };

  function renderChokepoint(id) {
    const cp = window.CHOKEPOINTS && window.CHOKEPOINTS[id];
    if (!cp) return;
    const threat = cp.threat || 'pending';
    const pct    = cp.threatPct == null ? 0 : cp.threatPct;
    gpEls.title.textContent = cp.name;
    gpEls.coords.textContent = `${cp.lat.toFixed(2)}°${cp.lat>=0?'N':'S'} · ${cp.lon.toFixed(2)}°${cp.lon>=0?'E':'W'}`;
    gpEls.fill.style.width = pct + '%';
    gpEls.tag.textContent = threat.toUpperCase();
    const threatColor = threat === 'critical' ? '#ff3d5e' : threat === 'high' ? '#ff8c42' : threat === 'safe' ? '#00e690' : threat === 'pending' ? '#556475' : '#ffab00';
    gpEls.tag.style.color = threatColor;
    gpEls.tag.style.borderColor = threatColor;
    gpEls.tag.style.background = threatColor + '14';
    gpEls.fill.style.background = `linear-gradient(90deg, ${threatColor}88, ${threatColor})`;
    gpEls.flow.textContent = cp.flowMbd != null ? cp.flowMbd : '—';
    gpEls.width.innerHTML = cp.widthMi != null ? `${cp.widthMi} <span>mi</span>` : '—';
    gpEls.vessels.textContent = cp.vesselsInZone != null ? cp.vesselsInZone : '—';
    gpEls.incidents.textContent = cp.incidents30d != null ? cp.incidents30d : '—';

    gpEls.routes.innerHTML = (cp.routes || []).map(r => `
      <li><span class="r-dot r-${r.risk}"></span>${r.from} → ${r.to} <b>${r.mbd} mbd</b></li>
    `).join('');

    renderVesselList(id, cp);
  }

  // ── Vessel list rendering — fed by live AISStream.io WebSocket via backend ──
  // Only populated for chokepoints that have a bounding box server-side (hormuz,
  // bab). Other chokepoints show a contextual message instead of a lying list.
  const LIVE_AIS_CHOKEPOINTS = new Set(['hormuz', 'bab']);
  function escapeHtml(s) {
    return String(s == null ? '' : s).replace(/[&<>"]/g, c => (
      { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]
    ));
  }
  function renderVesselList(id, cp) {
    if (!gpEls.vesselList) return;
    const vessels = Array.isArray(cp && cp.liveVessels) ? cp.liveVessels : [];
    const status = window.CP && window.CP.aisStatus;
    const hint = 'color:var(--text-mute);font-family:var(--mono);font-size:11px;padding:6px 2px';

    if (!LIVE_AIS_CHOKEPOINTS.has(id)) {
      gpEls.vesselList.innerHTML = `<div style="${hint}">Live AIS coverage is limited to Hormuz and Bab el-Mandeb.</div>`;
      return;
    }
    if (vessels.length) {
      gpEls.vesselList.innerHTML = vessels.map(v => {
        const type  = escapeHtml(v.type || 'OTHER');
        const name  = escapeHtml(v.name || `MMSI ${v.mmsi}`);
        const speed = v.speed == null ? '—' : v.speed.toFixed(1);
        return `<div class="vessel-row">
          <span class="v-type">${type}</span>
          <span class="v-name">${name}</span>
          <span class="v-speed">${speed} kt</span>
        </div>`;
      }).join('');
      return;
    }
    // No vessels yet — use the AIS status to explain why
    let msg = 'Waiting for AIS feed…';
    if (status) {
      if (!status.configured)        msg = 'AIS feed not configured (AISSTREAM_API_KEY missing).';
      else if (!status.connected)    msg = 'AIS feed reconnecting…' + (status.last_error ? ` (${status.last_error})` : '');
      else if (!status.last_message_ts) msg = 'Connected. Waiting for first position report…';
      else                           msg = 'No vessels currently in the kill zone.';
    }
    gpEls.vesselList.innerHTML = `<div style="${hint}">${escapeHtml(msg)}</div>`;
  }

  // Poll /api/vessels/{cp} on an interval. 30s is well under the 30-min vessel
  // TTL and below most intermittent-noise thresholds. Uses the shared API
  // cache (15s TTL) so a page that renders both panels only hits the network
  // once per cycle.
  async function refreshLiveVessels() {
    if (!window.API || !window.CHOKEPOINTS) return;
    try {
      const [status, hormuz, bab] = await Promise.all([
        window.API.vesselsStatus().catch(() => null),
        window.API.vessels('hormuz').catch(() => null),
        window.API.vessels('bab').catch(() => null),
      ]);
      window.CP = window.CP || {};
      window.CP.aisStatus = status;
      if (window.CHOKEPOINTS.hormuz) {
        window.CHOKEPOINTS.hormuz.liveVessels = (hormuz && hormuz.data) || [];
        const hUpd = (hormuz && hormuz.data) ? hormuz.data.length : null;
        if (hUpd != null) window.CHOKEPOINTS.hormuz.vesselsInZone = hUpd;
      }
      if (window.CHOKEPOINTS.bab) {
        window.CHOKEPOINTS.bab.liveVessels = (bab && bab.data) || [];
        const bUpd = (bab && bab.data) ? bab.data.length : null;
        if (bUpd != null) window.CHOKEPOINTS.bab.vesselsInZone = bUpd;
      }
      renderChokepoint(currentChokepoint);
    } catch (e) {
      // Swallow — the panel has a fallback message
      console.warn('AIS poll failed:', e);
    }
  }
  refreshLiveVessels();
  setInterval(refreshLiveVessels, 30_000);

  let currentChokepoint = 'hormuz';
  window.addEventListener('chokepoint-select', (e) => {
    currentChokepoint = e.detail;
    renderChokepoint(currentChokepoint);
  });
  window.addEventListener('data-hydrated', () => renderChokepoint(currentChokepoint));
  // Heavy ACLED payload arrives after data-hydrated — re-render the side
  // panel once real events are in so `INCIDENTS · 30D` stops showing 0.
  window.addEventListener('events-ready', () => renderChokepoint(currentChokepoint));
  renderChokepoint(currentChokepoint); // default

  // ═════════════════════════════════════════════════════════════════════
  // Timeline scrubber — interactive drag / click / keyboard
  // Track maps 0%→100% onto 2023-10-07 (Hamas attack) → today (2026-04-22).
  // ═════════════════════════════════════════════════════════════════════
  (function initTimeline() {
    const track  = document.querySelector('.ts-track');
    const tsFill = document.getElementById('tsFill');
    const tsThumb = document.getElementById('tsThumb');
    const tsDate  = document.getElementById('tsDate');
    const tsButtons = document.querySelectorAll('.ts-btn');
    if (!track || !tsFill || !tsThumb || !tsDate) return;

    const START = new Date('2023-10-07').getTime();
    const END   = new Date('2026-04-22').getTime();
    const SPAN  = END - START;

    // Preset positions (in pct) tied to real dates
    const epochMap = {
      pre:    { date: '2023-11-01', label: 'NOV 2023' },
      houthi: { date: '2023-12-01', label: 'DEC 2023' },
      war:    { date: '2026-02-28', label: 'FEB 2026' },
      now:    { date: '2026-04-22', label: '22 APR 2026' }
    };
    const dateToPct = iso => {
      const t = new Date(iso).getTime();
      return Math.max(0, Math.min(100, ((t - START) / SPAN) * 100));
    };
    const pctToDate = pct => new Date(START + (pct / 100) * SPAN);

    const MONTHS = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
    const fmtDate = d => `${String(d.getUTCDate()).padStart(2,'0')} ${MONTHS[d.getUTCMonth()]} ${d.getUTCFullYear()}`;

    let currentPct = 100; // default to "now"

    // Era boundaries used by all temporal-aware renderers
    const HOUTHI_START = new Date('2023-12-01').getTime();
    const WAR_START    = new Date('2026-02-28').getTime();
    function eraOf(ts) {
      if (ts < HOUTHI_START) return 'pre';
      if (ts < WAR_START)    return 'houthi';
      return 'war';
    }

    function setPct(pct, { fromButton = null, silent = false } = {}) {
      currentPct = Math.max(0, Math.min(100, pct));
      tsFill.style.width  = currentPct + '%';
      tsThumb.style.left  = currentPct + '%';
      const d = pctToDate(currentPct);
      tsDate.textContent = fmtDate(d);
      // De-activate all preset buttons unless one was explicitly picked
      tsButtons.forEach(b => {
        if (fromButton) {
          b.classList.toggle('active', b === fromButton);
        } else {
          b.classList.remove('active');
        }
      });
      try { localStorage.setItem('cp_timeline_pct', String(currentPct)); } catch (e) {}
      // Publish timeline state to the global bus so consumers (charts, globe,
      // tac-map, feed, KPIs) can read it synchronously without listening.
      const ts = d.getTime();
      window.CP = window.CP || {};
      window.CP.timeline = {
        pct: currentPct,
        date: d.toISOString().slice(0, 10),
        ts,
        era: eraOf(ts),
        // Is the current scrub at or near "today"? 0.05% of the ~2.5y span
        // is roughly half a day — tight enough that a deliberate scrub even
        // one notch back from the end disengages the "no filter" shortcut.
        atNow: currentPct >= 99.95
      };
      if (!silent) {
        window.dispatchEvent(new CustomEvent('timeline-set', { detail: window.CP.timeline }));
      }
    }

    // Preset button clicks
    tsButtons.forEach(btn => btn.addEventListener('click', () => {
      const e = epochMap[btn.dataset.epoch];
      if (e) setPct(dateToPct(e.date), { fromButton: btn });
    }));

    // Drag + click on track
    function xToPct(clientX) {
      const rect = track.getBoundingClientRect();
      return ((clientX - rect.left) / rect.width) * 100;
    }
    let dragging = false;
    function onDown(ev) {
      dragging = true;
      track.setPointerCapture && track.setPointerCapture(ev.pointerId);
      track.classList.add('ts-track-dragging');
      setPct(xToPct(ev.clientX));
      ev.preventDefault();
    }
    function onMove(ev) {
      if (!dragging) return;
      setPct(xToPct(ev.clientX));
    }
    function onUp(ev) {
      if (!dragging) return;
      dragging = false;
      track.classList.remove('ts-track-dragging');
      try { track.releasePointerCapture && track.releasePointerCapture(ev.pointerId); } catch (e) {}
    }
    track.addEventListener('pointerdown', onDown);
    track.addEventListener('pointermove', onMove);
    track.addEventListener('pointerup', onUp);
    track.addEventListener('pointercancel', onUp);
    // Fallback for mice/trackpads — click-to-jump
    track.addEventListener('click', ev => {
      if (ev.target.classList.contains('ts-event')) return; // handled below
      setPct(xToPct(ev.clientX));
    });

    // Click on an event marker jumps to its date
    track.querySelectorAll('.ts-event').forEach(ev => {
      ev.style.cursor = 'pointer';
      ev.addEventListener('click', e => {
        e.stopPropagation();
        const leftAttr = ev.getAttribute('style') || '';
        const m = leftAttr.match(/left:\s*(-?[\d.]+)%/);
        if (m) setPct(parseFloat(m[1]));
      });
    });

    // Keyboard — arrow keys when focused or hovering the scrubber
    const scrubSection = document.querySelector('.timeline-scrubber');
    if (scrubSection) {
      scrubSection.setAttribute('tabindex', '0');
      scrubSection.addEventListener('keydown', ev => {
        if (ev.key === 'ArrowLeft')  { setPct(currentPct - (ev.shiftKey ? 5 : 1)); ev.preventDefault(); }
        if (ev.key === 'ArrowRight') { setPct(currentPct + (ev.shiftKey ? 5 : 1)); ev.preventDefault(); }
        if (ev.key === 'Home')       { setPct(0); ev.preventDefault(); }
        if (ev.key === 'End')        { setPct(100); ev.preventDefault(); }
      });
    }

    // Make the thumb keyboard-focusable too
    tsThumb.setAttribute('tabindex', '0');
    tsThumb.setAttribute('role', 'slider');
    tsThumb.setAttribute('aria-valuemin', '0');
    tsThumb.setAttribute('aria-valuemax', '100');

    // Restore saved position
    let initial = 100;
    try {
      const saved = parseFloat(localStorage.getItem('cp_timeline_pct'));
      if (!isNaN(saved)) initial = saved;
    } catch (e) {}
    // Always broadcast the initial value so late-binding consumers (charts
    // re-rendered after hydration, globe re-rendering, etc.) see it.
    setPct(initial, { silent: false });
    // Highlight the preset that best matches initial
    let bestBtn = null, bestDiff = Infinity;
    tsButtons.forEach(b => {
      const e = epochMap[b.dataset.epoch]; if (!e) return;
      const diff = Math.abs(dateToPct(e.date) - initial);
      if (diff < bestDiff) { bestDiff = diff; bestBtn = b; }
    });
    if (bestBtn && bestDiff < 1.5) bestBtn.classList.add('active');
  })();

  // ── Data explorer overlay ──
  const overlay = document.getElementById('dataOverlay');
  const openBtn = document.getElementById('dataExplorerBtn');
  const closeBtn = document.getElementById('dpClose');
  if (openBtn) openBtn.addEventListener('click', () => overlay.classList.add('open'));
  if (closeBtn) closeBtn.addEventListener('click', () => overlay.classList.remove('open'));
  if (overlay) overlay.addEventListener('click', (e) => { if (e.target === overlay) overlay.classList.remove('open'); });

  // ── Data Explorer table — filters real ACLED events from window.CP.state.events ──
  const dpTbody  = document.getElementById('dpTbody');
  const dpSearch = document.getElementById('dpSearch');
  const dpType   = document.getElementById('dpType');
  const dpFrom   = document.getElementById('dpFrom');
  const dpTo     = document.getElementById('dpTo');
  const dpApply  = document.getElementById('dpApply');
  const dpExport = document.getElementById('dpExport');
  const dpMeta   = document.getElementById('dpMeta');
  const MAX_ROWS = 500;

  function dpEvents() {
    const s = window.CP && window.CP.state;
    return (s && Array.isArray(s.events) && s.events.length) ? s.events : (window.THESIS_EVENTS || []);
  }
  function dpFilter() {
    const events = dpEvents();
    const q = (dpSearch && dpSearch.value || '').trim().toLowerCase();
    const type = (dpType && dpType.value) || '';
    const from = dpFrom && dpFrom.value ? Date.parse(dpFrom.value) : null;
    const to   = dpTo   && dpTo.value   ? Date.parse(dpTo.value) + 86399999 : null;
    return events.filter(e => {
      if (type && (e.event_type || '') !== type) return false;
      const dt = e.event_date ? Date.parse(e.event_date) : NaN;
      if (!isNaN(dt)) {
        if (from != null && dt < from) return false;
        if (to   != null && dt > to)   return false;
      }
      if (q) {
        const hay = `${e.event_type||''} ${e.sub_event_type||''} ${e.actor1||''} ${e.location||e.admin1||''} ${e.notes||''}`.toLowerCase();
        if (!hay.includes(q)) return false;
      }
      return true;
    });
  }
  function dpRender() {
    if (!dpTbody) return;
    const filtered = dpFilter();
    const rows = filtered.slice(0, MAX_ROWS).map(e => {
      const d = e.event_date ? String(e.event_date).slice(0, 10) : '—';
      const t = e.event_type || '—';
      const s = e.sub_event_type || '—';
      const a = e.actor1 || '—';
      const l = e.location || e.admin1 || '—';
      const f = e.fatalities != null ? e.fatalities : 0;
      return `<tr><td>${d}</td><td>${t}</td><td>${s}</td><td>${a}</td><td>${l}</td><td>${f}</td></tr>`;
    });
    dpTbody.innerHTML = rows.join('') || '<tr><td colspan="6" style="text-align:center;color:var(--text-mute);padding:24px">No matching events</td></tr>';
    if (dpMeta) {
      const cap = filtered.length > MAX_ROWS ? ` · showing first ${MAX_ROWS}` : '';
      dpMeta.textContent = `${filtered.length.toLocaleString()} EVENT${filtered.length === 1 ? '' : 'S'}${cap}`;
    }
  }
  function dpPopulateTypes() {
    if (!dpType) return;
    const events = dpEvents();
    const set = new Set();
    for (const e of events) if (e.event_type) set.add(e.event_type);
    const types = [...set].sort();
    const cur = dpType.value;
    dpType.innerHTML = '<option value="">All</option>' +
      types.map(t => `<option value="${t.replace(/"/g, '&quot;')}">${t}</option>`).join('');
    if (cur && set.has(cur)) dpType.value = cur;
  }
  function dpExportCsv() {
    const filtered = dpFilter();
    const head = ['event_date','event_type','sub_event_type','actor1','location','admin1','fatalities','latitude','longitude'];
    const esc = v => {
      if (v == null) return '';
      const s = String(v);
      return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
    };
    const lines = [head.join(',')];
    for (const e of filtered) lines.push(head.map(k => esc(e[k])).join(','));
    const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `events_${new Date().toISOString().slice(0,10)}.csv`;
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 0);
  }
  if (dpApply)  dpApply.addEventListener('click', dpRender);
  if (dpExport) dpExport.addEventListener('click', dpExportCsv);
  if (dpSearch) dpSearch.addEventListener('keydown', e => { if (e.key === 'Enter') dpRender(); });
  // Initial render (will show empty / placeholder until events arrive)
  dpRender();
  // Re-render when events fetch resolves
  window.addEventListener('events-ready', () => { dpPopulateTypes(); dpRender(); });
  window.addEventListener('thesis-events-ready', () => { dpPopulateTypes(); dpRender(); });
  window.addEventListener('data-hydrated', () => { dpPopulateTypes(); dpRender(); });

  // ── Tactical map filter toggles ──
  function wireTacToggles() {
    const toggles = document.querySelectorAll('.tac-toggle input');
    if (!toggles.length) return;
    const layers = window.__tacLayers;
    if (!layers) { setTimeout(wireTacToggles, 300); return; }
    toggles.forEach(t => {
      t.addEventListener('change', applyTacFilters);
    });
    applyTacFilters();
  }

  function applyTacFilters() {
    const layers = window.__tacLayers;
    if (!layers) return;
    const toggles = document.querySelectorAll('.tac-toggle input');
    const state = {};
    toggles.forEach(t => {
      const lbl = t.parentElement.textContent.trim().toLowerCase();
      state[lbl] = t.checked;
    });
    // Era from temporal scrubber (default to 'now' when nothing scrubbed yet)
    const era = (window.CP && window.CP.timeline && window.CP.timeline.era) || 'now';
    const preCrisis = era === 'pre';
    // Heatmap
    layers.heatLayer.style('display', (state['heatmap'] && !preCrisis) ? null : 'none');
    // Chokepoint zone
    layers.chokeLayer.style('display', state['chokepoint zone'] ? null : 'none');
    // Strike markers + tanker-only — pre-crisis era hides every marker because
    // the Houthi maritime campaign hadn't started yet.
    const markers = layers.markerLayer.selectAll('g.atk');
    if (!state['strike markers'] || preCrisis) {
      markers.style('display', 'none');
    } else {
      markers.style('display', d => {
        if (state['tanker-only'] && d.type !== 'tanker') return 'none';
        return null;
      });
    }
    // NOTE: Side-panel stats reflect the full ACLED dataset (N=726) from the
    // thesis and are intentionally NOT recomputed from the visible illustrative
    // markers. Toggles only affect map layer visibility.
  }

  // Expose for the timeline listener so scrubbing can re-run it
  window.applyTacFilters = applyTacFilters;
  window.addEventListener('timeline-set', () => applyTacFilters());

  // Run after tac map has loaded its data
  setTimeout(wireTacToggles, 800);

  // ── Live clock ──
  function updateClock() {
    const el = document.getElementById('statusTime');
    if (!el) return;
    const d = new Date();
    const months = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
    const dd = String(d.getUTCDate()).padStart(2,'0');
    const mm = months[d.getUTCMonth()];
    const yy = d.getUTCFullYear();
    const hh = String(d.getUTCHours()).padStart(2,'0');
    const mi = String(d.getUTCMinutes()).padStart(2,'0');
    el.textContent = `${dd} ${mm} ${yy} · ${hh}:${mi}Z`;
  }
  updateClock();
  setInterval(updateClock, 30000);

  // ── Live vessel traffic (AIS embeds) ──
  // MarineTraffic's free embed sometimes gets blocked by strict CSP /
  // X-Frame-Options on corporate networks. We can't read the inner document
  // cross-origin, but we CAN time-out: if `load` hasn't fired within a
  // window the frame is almost certainly blocked, so reveal the fallback.
  // When it does load, we keep the fallback hidden.
  function wireVesselEmbed(frameId, wrapId) {
    const frame = document.getElementById(frameId);
    const wrap  = document.getElementById(wrapId);
    if (!frame || !wrap) return;
    let loaded = false;
    frame.addEventListener('load', () => {
      loaded = true;
      wrap.classList.remove('embed-blocked');
    });
    frame.addEventListener('error', () => {
      wrap.classList.add('embed-blocked');
    });
    // Safety net: if no load event after 8s, assume the embed was blocked
    // (e.g. `frame-ancestors` CSP, corporate proxy). Don't mark earlier —
    // MarineTraffic's embed can take a few seconds to paint on slow links.
    setTimeout(() => {
      if (!loaded) wrap.classList.add('embed-blocked');
    }, 8000);
  }
  wireVesselEmbed('vtHormuzFrame', 'vtHormuzWrap');
  wireVesselEmbed('vtBabFrame',    'vtBabWrap');

  // ── Vessel-class filter chips (MarineTraffic) ──
  // The /ais/embed/ route ignores the `vtypes` URL segment in practice (the
  // embed always renders the full fleet, regardless of what we put in the
  // URL). The /ais/home/ fullscreen route DOES honor it. So each chip is an
  // anchor that opens MarineTraffic's fullscreen view in a new tab with the
  // chosen vessel class applied — that gives the user a reliable way to
  // de-clutter the map without pretending the iframe filter works.
  // Codes: 6 = passenger, 7 = tankers, 8 = cargo. Pipe-separate ("7|8") to
  // combine.
  function wireVesselFilters() {
    const filters = document.querySelectorAll('.vt-filter[data-fs-base]');
    filters.forEach(filter => {
      const base = filter.dataset.fsBase;
      if (!base) return;
      filter.querySelectorAll('.vt-chip[data-vtypes]').forEach(chip => {
        const vtypes = chip.dataset.vtypes || '';
        chip.href = vtypes ? `${base}/vtypes:${vtypes}` : base;
      });
    });
  }
  wireVesselFilters();
});
