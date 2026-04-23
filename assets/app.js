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

    gpEls.vesselList.innerHTML = cp.vessels.length ? cp.vessels.map(v => `
      <div class="vessel-row">
        <span class="v-type">${v.type}</span>
        <span class="v-name">${v.name}</span>
        <span class="v-speed">${v.speed} kt</span>
      </div>
    `).join('') : '<div style="color:var(--text-mute);font-family:var(--mono);font-size:11px">No vessels tracked</div>';
  }

  let currentChokepoint = 'hormuz';
  window.addEventListener('chokepoint-select', (e) => {
    currentChokepoint = e.detail;
    renderChokepoint(currentChokepoint);
  });
  window.addEventListener('data-hydrated', () => renderChokepoint(currentChokepoint));
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
        // Is the current scrub at or near "today"? Used to suppress filtering
        // when the user has not actually scrubbed back in time.
        atNow: currentPct >= 99.5
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

  // Mock data rows
  const dpTbody = document.getElementById('dpTbody');
  if (dpTbody) {
    const types = ['Explosions/Remote violence','Battles','Strategic developments','Violence against civilians'];
    const subs = ['Shelling/artillery','Air/drone strike','Armed clash','Attack'];
    const actors = ['Houthis','Military Forces of Yemen','IRGC','US Navy','Israeli Forces'];
    const locs = ['Hodeidah','Sanaa','Strait of Hormuz','Bandar Abbas','Eilat'];
    const rows = [];
    for (let i=0;i<80;i++) {
      const d = new Date(); d.setDate(d.getDate() - Math.floor(Math.random()*120));
      rows.push(`<tr>
        <td>${d.toISOString().slice(0,10)}</td>
        <td>${types[i%types.length]}</td>
        <td>${subs[i%subs.length]}</td>
        <td>${actors[i%actors.length]}</td>
        <td>${locs[i%locs.length]}</td>
        <td>${Math.random()<0.3?Math.floor(Math.random()*8):0}</td>
      </tr>`);
    }
    dpTbody.innerHTML = rows.join('');
  }

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
});
