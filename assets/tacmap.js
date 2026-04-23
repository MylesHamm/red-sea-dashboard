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

    // Real attack markers — approximate lat/lon for recorded incidents (ACLED-style)
    const attacks = [
      { lat: 12.72, lon: 43.25, type: 'tanker', intensity: 'high' },
      { lat: 12.60, lon: 43.40, type: 'tanker', intensity: 'high' },
      { lat: 13.20, lon: 42.80, type: 'drone',  intensity: 'high' },
      { lat: 14.10, lon: 42.50, type: 'missile',intensity: 'high' },
      { lat: 14.85, lon: 42.60, type: 'tanker', intensity: 'med' },
      { lat: 15.30, lon: 41.80, type: 'drone',  intensity: 'med' },
      { lat: 16.10, lon: 41.30, type: 'missile',intensity: 'med' },
      { lat: 17.50, lon: 40.20, type: 'drone',  intensity: 'low' },
      { lat: 18.70, lon: 39.50, type: 'tanker', intensity: 'low' },
      { lat: 20.40, lon: 38.50, type: 'drone',  intensity: 'med' },
      { lat: 12.90, lon: 43.70, type: 'tanker', intensity: 'high' },
      { lat: 13.50, lon: 43.10, type: 'missile',intensity: 'high' },
      { lat: 14.50, lon: 42.30, type: 'drone',  intensity: 'med' },
      { lat: 13.00, lon: 44.20, type: 'drone',  intensity: 'med' },
      { lat: 11.90, lon: 44.10, type: 'missile',intensity: 'low' },
      { lat: 19.00, lon: 38.90, type: 'drone',  intensity: 'low' },
      { lat: 22.50, lon: 37.20, type: 'tanker', intensity: 'low' }
    ];
    // Expose for filter wiring & stats
    window.TAC_ATTACKS = attacks;
    window.__tacLayers = { heatLayer, chokeLayer, markerLayer, labelLayer };

    const color = t => t === 'tanker' ? '#ff3d5e' : t === 'missile' ? '#ff8c42' : '#ffab00';
    const rSize = i => i === 'high' ? 4 : i === 'med' ? 3 : 2.2;

    const g = markerLayer.selectAll('g.atk')
      .data(attacks).enter().append('g')
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
    // Markers are representative geographic samples drawn from the ACLED
    // events visible in the data tab — not a real-time AIS / threat feed.
    labelLayer.append('text')
      .attr('x', 18).attr('y', H - 14)
      .attr('font-family',"'JetBrains Mono', monospace").attr('font-size',8).attr('fill','#556475').attr('letter-spacing',1.5)
      .text('MARKERS · ILLUSTRATIVE · derived from ACLED 2024–2026');
  }
})();
