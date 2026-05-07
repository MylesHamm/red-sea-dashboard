/* Red Sea Tactical Map — D3 regional projection of the Gulf of Aden / Red Sea
   Uses same world-atlas land topojson, clipped Mercator projection.
   Plots real attack incidents at lat/lon, chokepoint box, heatmap blooms.
*/

(function(){
  'use strict';
  const svg = d3.select('#tacMapSvg');
  if (svg.empty()) return;

  const W = 800, H = 440;

  // Region of interest: Red Sea + Gulf of Aden + Bab el-Mandeb
  // Longitude 32 → 56 (East), Latitude 5 → 30 (North)
  const projection = d3.geoMercator()
    .center([43.5, 16])
    .scale(1100)
    .translate([W/2, H/2]);
  const path = d3.geoPath(projection);

  // Defs
  const defs = svg.append('defs');
  // ocean grid
  defs.append('pattern').attr('id','tacGrid').attr('width',40).attr('height',40).attr('patternUnits','userSpaceOnUse')
    .append('path').attr('d','M 40 0 L 0 0 0 40').attr('fill','none').attr('stroke','rgba(0,212,255,0.06)').attr('stroke-width',0.5);
  // heat blooms
  const h1 = defs.append('radialGradient').attr('id','tacHeat1').attr('cx','50%').attr('cy','50%').attr('r','50%');
  h1.append('stop').attr('offset','0%').attr('stop-color','#ff3d5e').attr('stop-opacity',0.55);
  h1.append('stop').attr('offset','100%').attr('stop-color','#ff3d5e').attr('stop-opacity',0);
  const h2 = defs.append('radialGradient').attr('id','tacHeat2').attr('cx','50%').attr('cy','50%').attr('r','50%');
  h2.append('stop').attr('offset','0%').attr('stop-color','#ffab00').attr('stop-opacity',0.45);
  h2.append('stop').attr('offset','100%').attr('stop-color','#ffab00').attr('stop-opacity',0);
  // pulse marker
  defs.append('radialGradient').attr('id','tacPulse').attr('cx','50%').attr('cy','50%').attr('r','50%')
    .selectAll('stop').data([
      {o:'0%', c:'#ff3d5e', op:0.8},
      {o:'60%', c:'#ff3d5e', op:0.15},
      {o:'100%', c:'#ff3d5e', op:0}
    ]).enter().append('stop').attr('offset',d=>d.o).attr('stop-color',d=>d.c).attr('stop-opacity',d=>d.op);

  // ocean background
  svg.append('rect').attr('width',W).attr('height',H).attr('fill','#050a12');
  svg.append('rect').attr('width',W).attr('height',H).attr('fill','url(#tacGrid)');

  // layers
  const landLayer = svg.append('g');
  const heatLayer = svg.append('g');
  const chokeLayer = svg.append('g');
  const routeLayer = svg.append('g');
  const markerLayer = svg.append('g');
  const labelLayer = svg.append('g');

  // Load topojson
  fetch('https://cdn.jsdelivr.net/npm/world-atlas@2/land-110m.json')
    .then(r => r.json())
    .then(world => {
      const land = topojson.feature(world, world.objects.land);
      landLayer.append('path')
        .datum(land)
        .attr('d', path)
        .attr('fill','#0a1523')
        .attr('stroke','rgba(0,212,255,0.28)')
        .attr('stroke-width',0.8);
      drawOverlay();
    })
    .catch(() => drawOverlay());

  function drawOverlay(){
    // Heat blooms around known flashpoints (real lat/lon)
    const heats = [
      { lat: 12.6, lon: 43.3, r: 85, grad: 'tacHeat1' }, // Bab el-Mandeb
      { lat: 14.8, lon: 42.6, r: 70, grad: 'tacHeat1' }, // Hodeidah approaches
      { lat: 20.3, lon: 38.8, r: 55, grad: 'tacHeat2' }, // central Red Sea
      { lat: 16.5, lon: 41.5, r: 60, grad: 'tacHeat2' }  // S Red Sea
    ];
    heatLayer.selectAll('circle')
      .data(heats).enter().append('circle')
      .attr('cx', d => projection([d.lon, d.lat])[0])
      .attr('cy', d => projection([d.lon, d.lat])[1])
      .attr('r', d => d.r)
      .attr('fill', d => `url(#${d.grad})`);

    // Chokepoint zone box (Bab el-Mandeb strait area)
    const tl = projection([42.8, 13.3]);
    const br = projection([43.8, 12.0]);
    chokeLayer.append('rect')
      .attr('x', tl[0]).attr('y', tl[1])
      .attr('width', br[0]-tl[0]).attr('height', br[1]-tl[1])
      .attr('fill','rgba(255,171,0,0.04)')
      .attr('stroke','#ffab00').attr('stroke-width',1).attr('stroke-dasharray','4 3');
    labelLayer.append('text')
      .attr('x', (tl[0]+br[0])/2).attr('y', tl[1]-6)
      .attr('text-anchor','middle')
      .attr('font-family',"'JetBrains Mono', monospace")
      .attr('font-size',10).attr('fill','#ffab00').attr('letter-spacing',2)
      .text('BAB EL-MANDEB · 18 mi');

    // Suez label
    const suez = projection([32.55, 30.0]);
    labelLayer.append('circle').attr('cx', suez[0]).attr('cy', suez[1]).attr('r',3).attr('fill','#00d4ff');
    labelLayer.append('text')
      .attr('x', suez[0]+8).attr('y', suez[1]+4)
      .attr('font-family',"'JetBrains Mono', monospace").attr('font-size',9).attr('fill','#8f9db0')
      .text('SUEZ');

    // Port labels
    const ports = [
      { name: 'JEDDAH', lat: 21.49, lon: 39.19 },
      { name: 'ADEN',   lat: 12.78, lon: 45.03 },
      { name: 'HODEIDAH', lat: 14.8, lon: 42.95 },
      { name: 'DJIBOUTI', lat: 11.58, lon: 43.15 },
      { name: 'PORT SUDAN', lat: 19.62, lon: 37.22 }
    ];
    const portG = labelLayer.selectAll('g.port').data(ports).enter().append('g');
    portG.append('circle')
      .attr('cx', d => projection([d.lon,d.lat])[0])
      .attr('cy', d => projection([d.lon,d.lat])[1])
      .attr('r', 2.5).attr('fill','#00d4ff').attr('stroke','rgba(0,212,255,0.4)').attr('stroke-width',3);
    portG.append('text')
      .attr('x', d => projection([d.lon,d.lat])[0] + 6)
      .attr('y', d => projection([d.lon,d.lat])[1] + 3)
      .attr('font-family',"'JetBrains Mono', monospace").attr('font-size',9)
      .attr('fill','#8f9db0').attr('letter-spacing',1)
      .text(d => d.name);

    // Shipping lane (Red Sea corridor) — follows the water
    const lane = [
      [32.55, 30.0], [34.5, 27.5], [36, 25.5], [38, 22.5],
      [39.5, 19.5], [40.5, 17], [41.5, 15], [42.5, 13.5],
      [43.3, 12.6], [44.5, 12.5], [47, 12.3], [50, 13], [52, 14]
    ];
    routeLayer.append('path')
      .attr('d', path({ type: 'LineString', coordinates: lane }))
      .attr('fill','none')
      .attr('stroke','#00d4ff').attr('stroke-width',1.2)
      .attr('stroke-opacity',0.5).attr('stroke-dasharray','3 5');

    // Real attack markers — pulled directly from the live ACLED feed. We
    // filter to Houthi-attributed events within the Red Sea / Gulf of Aden
    // viewport, map ACLED event categories onto the three marker classes
    // (tanker / missile / drone), and rebuild on `events-ready` when the
    // heavy /api/events payload arrives. If events haven't loaded yet (or
    // the dataset is empty for the selected time window) the marker layer
    // is empty — we never synthesize placeholder positions.
    const color = t => t === 'tanker' ? '#ff3d5e' : t === 'missile' ? '#ff8c42' : '#ffab00';
    const rSize = i => i === 'high' ? 4 : i === 'med' ? 3 : 2.2;

    // ── Maritime/oil-impact filter ──────────────────────────────────────
    // §02.1 is the "Red Sea Maritime Kill Zone" view — its stated purpose
    // is to surface events that impact shipping / oil flow. The raw
    // Houthi-attributed ACLED stream is dominated by inland Yemeni
    // civil-war activity (Houthi vs. government forces, internal
    // battles, urban unrest) that has no maritime relevance.
    //
    // An event qualifies as maritime-impactful if it mentions:
    //   (a) a maritime target/asset (tanker, vessel, ship, freighter,
    //       cargo, frigate, destroyer, dhow, MV/USS named vessels),
    //   (b) a maritime context (sea, naval, port, harbor, anchored,
    //       shipping, transit, strait, anti-ship), OR
    //   (c) a Houthi maritime-port location (Hodeidah, Mokha, Ras
    //       Issa, Salif — the staging ports for their maritime ops).
    //
    // sub_event_type "Air/drone strike" with a maritime target counts;
    // a generic Houthi-vs-government infantry battle in inland Yemen
    // does not.
    const MARITIME_TARGETS = [
      'tanker', 'tankers', 'vessel', 'vessels', 'ship', 'ships', 'shipping',
      'freighter', 'cargo', 'container', 'merchant ship', 'merchant vessel',
      'dhow', 'frigate', 'destroyer', 'carrier', 'warship', 'fleet',
      ' mv ', ' uss ', 'galaxy leader', 'true confidence',
    ];
    const MARITIME_CONTEXT = [
      'naval', 'navy', 'maritime', 'port', 'harbor', 'harbour', 'anchored',
      'anchorage', 'transit', 'sea lane', 'sea-lane', 'shipping lane',
      'strait', 'red sea', 'gulf of aden', 'bab al-mandab', 'bab el-mandeb',
      'bab al-mandeb', 'anti-ship', 'anti ship', 'sea-launched',
      'usv', 'sea drone', 'torpedo', 'mine',
    ];
    const MARITIME_PORTS = [
      'hodeidah', 'hudaydah', 'hodeida', 'mokha', 'ras issa', 'salif',
      'aden', 'al hudaydah',
    ];

    // Maritime classification is done SERVER-SIDE in app.py's
    // /api/events handler (so the lite payload — which strips `notes`
    // — still carries the flags). The functions below just read the
    // pre-computed boolean fields.
    function isMaritimeRelevant(e) {
      // Trust the server-side flag if present
      if (typeof e.maritime === 'boolean') return e.maritime;
      // Fallback for older payloads that lack the flag — keyword match
      // on whatever fields ARE present (sub_event_type + location).
      const hay = (
        (e.sub_event_type || '') + ' ' +
        (e.location || '') + ' ' +
        (e.notes || '')
      ).toLowerCase();
      const padded = ' ' + hay + ' ';
      for (const k of MARITIME_TARGETS)  if (padded.includes(k)) return true;
      for (const k of MARITIME_CONTEXT)  if (padded.includes(k)) return true;
      for (const k of MARITIME_PORTS)    if (padded.includes(k)) return true;
      return false;
    }
    // Expose for hydrate.js so the side-panel stats apply the same rule.
    window.__isMaritimeRelevant = isMaritimeRelevant;

    function isTankerSpecific(e) {
      if (typeof e.tanker_target === 'boolean') return e.tanker_target;
      // No fallback path — without notes (lite payload) we can't infer
      // tanker-targeting client-side. The server-side flag is required.
      return false;
    }
    window.__isTankerSpecific = isTankerSpecific;

    function classifyEvent(e) {
      const hay = (
        (e.sub_event_type || '') + ' ' +
        (e.event_type || '') + ' ' +
        (e.notes || '') + ' ' +
        (e.source || '')
      ).toLowerCase();
      if (hay.includes('naval') || hay.includes('tanker') || hay.includes('vessel') || hay.includes('ship') || hay.includes('boarding') || hay.includes('hijack')) return 'tanker';
      if (hay.includes('drone') || hay.includes('uav') || hay.includes('usv') || hay.includes('unmanned')) return 'drone';
      if (hay.includes('missile') || hay.includes('shelling') || hay.includes('artillery') || hay.includes('rocket')) return 'missile';
      if ((e.event_type || '').toLowerCase().includes('explosion')) return 'missile';
      return 'drone';
    }

    function classifyIntensity(e) {
      const f = +e.fatalities || 0;
      if (f >= 3) return 'high';
      if (f >= 1) return 'med';
      return 'low';
    }

    function isHouthi(e) {
      const a = ((e.actor1 || '') + ' ' + (e.actor2 || '')).toLowerCase();
      return a.includes('houthi') || a.includes('ansar allah');
    }

    // Viewport bounds — match the projection's visible area (32°E–56°E, 5°N–30°N).
    function inViewport(lat, lon) {
      return lon >= 32 && lon <= 56 && lat >= 5 && lat <= 30;
    }

    // Filter mode state (persisted via UI toggles below).
    //   'tanker' (default)    — vessel-targeting events ONLY (Houthi
    //                            attacks on ships, hijackings, named-
    //                            vessel incidents, anti-ship infrastructure
    //                            strikes). The tightest oil-impact subset.
    //   'maritime'            — broader: also includes US/UK counter-Houthi
    //                            strikes mentioning anti-ship context.
    //   'all'                 — every Houthi-attributed event in the
    //                            viewport (incl. inland civil-war).
    window.__tacFilterMode = window.__tacFilterMode || 'tanker';

    function eventsToAttacks(events) {
      if (!Array.isArray(events)) return [];
      const mode = window.__tacFilterMode || 'maritime';
      const out = [];
      for (const e of events) {
        if (!isHouthi(e)) continue;
        const lat = +e.latitude, lon = +e.longitude;
        if (!lat || !lon) continue;
        if (!inViewport(lat, lon)) continue;
        // Maritime-impact filter — see isMaritimeRelevant() above for
        // the keyword set. The §02.1 panel's purpose is the maritime
        // kill zone, so the default mode excludes the inland-Yemen
        // civil-war noise that dominates the raw Houthi stream.
        if (mode === 'maritime' && !isMaritimeRelevant(e)) continue;
        if (mode === 'tanker'   && !isTankerSpecific(e))  continue;
        out.push({
          lat, lon,
          type: classifyEvent(e),
          intensity: classifyIntensity(e),
          date: e.event_date || e.date,
          notes: e.notes || '',
          fatalities: +e.fatalities || 0,
        });
      }
      return out;
    }
    window.__tacEventsToAttacks = eventsToAttacks;

    function renderAttacks(attacks) {
      window.TAC_ATTACKS = attacks;
      const sel = markerLayer.selectAll('g.atk').data(attacks, (d, i) => `${d.lat}|${d.lon}|${d.date || i}`);
      sel.exit().remove();
      const g = sel.enter().append('g')
        .attr('class','atk')
        .attr('data-atk-type', d => d.type);
      g.append('circle')
        .attr('cx', d => projection([d.lon,d.lat])[0])
        .attr('cy', d => projection([d.lon,d.lat])[1])
        .attr('r', d => rSize(d.intensity) + 4)
        .attr('fill','none')
        .attr('stroke', d => color(d.type))
        .attr('stroke-opacity', 0.3)
        .attr('class','tac-pulse');
      g.append('circle')
        .attr('cx', d => projection([d.lon,d.lat])[0])
        .attr('cy', d => projection([d.lon,d.lat])[1])
        .attr('r', d => rSize(d.intensity))
        .attr('fill', d => color(d.type))
        .attr('stroke', '#fff').attr('stroke-width', 0.5);
    }

    window.__tacLayers = { heatLayer, chokeLayer, markerLayer, labelLayer };
    window.__tacRenderAttacks = renderAttacks;
    window.__tacEventsToAttacks = eventsToAttacks;

    // If events already loaded by the time we got here, render immediately.
    const earlyEvents = (window.CP && window.CP.data && window.CP.data.events);
    if (earlyEvents && earlyEvents.length) {
      renderAttacks(eventsToAttacks(earlyEvents));
    }
    // Re-render whenever new ACLED events land.
    window.addEventListener('events-ready', (ev) => {
      const events = (ev && ev.detail && ev.detail.events) || [];
      renderAttacks(eventsToAttacks(events));
    });

    // ── Filter-mode chips (Maritime / Tanker only / All Houthi) ────────
    // Wires the data-tac-mode-chips buttons to the global filter state
    // and re-renders both the markers AND the side-panel stats.
    function applyTacMode(mode) {
      window.__tacFilterMode = mode;
      // Update chip active styling
      document.querySelectorAll('[data-tac-mode]').forEach(btn => {
        btn.classList.toggle('active', btn.getAttribute('data-tac-mode') === mode);
      });
      // Update header badge
      const badge = document.querySelector('[data-tac-mode-badge]');
      if (badge) {
        const labels = { tanker:   'ACLED · MARITIME ATTACKS',
                         maritime: 'ACLED · + COUNTER-STRIKES',
                         all:      'ACLED · HOUTHI · ALL' };
        badge.textContent = labels[mode] || labels.tanker;
      }
      // Re-render markers from the cached event list
      const events = (window.CP && window.CP.state && window.CP.state.events) || [];
      if (events.length && typeof window.__tacRenderAttacks === 'function') {
        window.__tacRenderAttacks(eventsToAttacks(events));
      }
      // Recompute side-panel stats with the new filter
      if (typeof window.__hydrateTacStats === 'function') {
        window.__hydrateTacStats(events);
      }
    }
    window.__tacApplyMode = applyTacMode;

    // Click handlers
    document.querySelectorAll('[data-tac-mode]').forEach(btn => {
      btn.addEventListener('click', (ev) => {
        ev.preventDefault();
        applyTacMode(btn.getAttribute('data-tac-mode'));
      });
    });

    // ── Layer toggles (Heatmap / Strike markers / Chokepoint zone) ─────
    function applyLayerVisibility(layer, on) {
      const map = {
        heat:    () => heatLayer.style('display', on ? null : 'none'),
        markers: () => markerLayer.style('display', on ? null : 'none'),
        zone:    () => chokeLayer.style('display', on ? null : 'none'),
      };
      if (map[layer]) map[layer]();
    }
    document.querySelectorAll('[data-tac-layer]').forEach(input => {
      input.addEventListener('change', () => {
        applyLayerVisibility(input.getAttribute('data-tac-layer'), input.checked);
      });
    });

    // ── Cross-filter: dim non-matching attack markers when a doughnut filter
    //    is active. ACLED categories don't map perfectly onto tanker/missile/
    //    drone, so the bridge in charts.js (xfMatchesTacType) handles fuzzy
    //    routing. When no match exists in the chosen category we fade all so
    //    the user sees the filter is engaged.
    function applyXfTac(filter) {
      const matches = t => {
        if (!window.xfMatchesTacType) return true;
        return window.xfMatchesTacType(filter, t);
      };
      markerLayer.selectAll('g.atk')
        .transition().duration(220)
        .style('opacity', d => matches(d.type) ? 1 : 0.18);
      // Floating pill on the tac-map container so users see why markers faded
      const wrap = svg.node() && svg.node().parentElement;
      if (wrap) {
        if (getComputedStyle(wrap).position === 'static') wrap.style.position = 'relative';
        let pill = wrap.querySelector('.xf-banner-tac');
        if (!filter) { if (pill) pill.remove(); return; }
        if (!pill) {
          pill = document.createElement('div');
          pill.className = 'xf-banner-tac';
          pill.style.cssText = 'position:absolute;top:8px;right:8px;padding:3px 7px;font:9px "JetBrains Mono",monospace;letter-spacing:1.5px;border-radius:3px;background:rgba(0,212,255,0.15);color:#dfe7f0;border:1px solid rgba(0,212,255,0.45);z-index:5;pointer-events:none';
          wrap.appendChild(pill);
        }
        pill.textContent = `FILTER: ${filter.toUpperCase()}`;
      }
    }
    // Pick up any filter that's already active on first paint
    applyXfTac((window.CP && window.CP.filters && window.CP.filters.eventType) || null);
    if (!window.__tacXfWired) {
      window.__tacXfWired = true;
      window.addEventListener('cross-filter-changed', e => {
        applyXfTac((e.detail && e.detail.eventType) || null);
      });
    }

    // Compass / scale
    labelLayer.append('text')
      .attr('x', 18).attr('y', 20)
      .attr('font-family',"'JetBrains Mono', monospace").attr('font-size',10).attr('fill','#3a4756').attr('letter-spacing',2)
      .text('RED SEA · GULF OF ADEN · 32°E–56°E');
    labelLayer.append('text')
      .attr('x', 18).attr('y', H - 28)
      .attr('font-family',"'JetBrains Mono', monospace").attr('font-size',9).attr('fill','#3a4756').attr('letter-spacing',2)
      .text('● TANKER  ● MISSILE  ● DRONE');
    // Footer label reflects live data source — each marker is a real ACLED
    // event geocoded to its reported lat/lon. Category is inferred from
    // sub_event_type + notes keywords; intensity from fatalities count.
    labelLayer.append('text')
      .attr('x', 18).attr('y', H - 14)
      .attr('font-family',"'JetBrains Mono', monospace").attr('font-size',8).attr('fill','#556475').attr('letter-spacing',1.5)
      .text('MARKERS · LIVE ACLED · Houthi-attributed events · refreshed with feed');
  }
})();
