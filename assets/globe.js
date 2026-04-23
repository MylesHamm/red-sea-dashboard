/* Rotating globe with shipping lanes — D3 orthographic projection
   - Auto-rotate (paused on hover)
   - Click chokepoint → dispatches 'chokepoint-select' event
   - Drag to rotate manually
*/

(function(){
  'use strict';

  const WORLD_URL = 'https://cdn.jsdelivr.net/npm/world-atlas@2/land-110m.json';

  const svg = d3.select('#globeSvg');
  if (svg.empty()) return;

  const width = 720, height = 720;
  const projection = d3.geoOrthographic()
    .scale(320)
    .translate([width/2, height/2])
    .clipAngle(90)
    .rotate([-50, -15]);

  const path = d3.geoPath(projection);

  // Defs: sphere gradient, glow filters
  const defs = svg.append('defs');

  const sphereGrad = defs.append('radialGradient')
    .attr('id','sphereGrad').attr('cx','35%').attr('cy','35%').attr('r','75%');
  sphereGrad.append('stop').attr('offset','0%').attr('stop-color','#0f1f30');
  sphereGrad.append('stop').attr('offset','70%').attr('stop-color','#050a14');
  sphereGrad.append('stop').attr('offset','100%').attr('stop-color','#02050b');

  const glow = defs.append('filter').attr('id','glow')
    .attr('x','-50%').attr('y','-50%').attr('width','200%').attr('height','200%');
  glow.append('feGaussianBlur').attr('stdDeviation','2').attr('result','b');
  const merge = glow.append('feMerge');
  merge.append('feMergeNode').attr('in','b');
  merge.append('feMergeNode').attr('in','SourceGraphic');

  // Base sphere
  svg.append('path')
    .attr('class','globe-sphere')
    .datum({type:'Sphere'})
    .attr('d', path);

  // Graticule
  const graticule = d3.geoGraticule10();
  svg.append('path')
    .attr('class','globe-graticule')
    .datum(graticule)
    .attr('d', path);

  const landLayer = svg.append('g').attr('class','land-layer');
  const arcLayer = svg.append('g').attr('class','arc-layer');
  const flowLayer = svg.append('g').attr('class','flow-layer');
  const nodeLayer = svg.append('g').attr('class','node-layer');
  const particleLayer = svg.append('g').attr('class','particle-layer');

  // Load world
  fetch(WORLD_URL).then(r => r.json()).then(world => {
    const land = topojson.feature(world, world.objects.land);
    landLayer.selectAll('path')
      .data([land])
      .enter().append('path')
      .attr('class','globe-land')
      .attr('d', path);
    render();
  }).catch(err => {
    console.warn('World atlas failed to load, globe will render without landmasses', err);
    render();
  });

  // Auto-rotate
  let rotation = [-50, -15];
  let autoRotate = true;
  let speed = 0.12;
  let lastTime = performance.now();

  function tick(now){
    const dt = now - lastTime; lastTime = now;
    if (autoRotate) {
      rotation[0] += speed * (dt / 16);
      projection.rotate(rotation);
      render();
    }
    animateParticles(now);
    requestAnimationFrame(tick);
  }
  requestAnimationFrame(tick);

  // Pause on hover
  const stage = document.getElementById('globeWrap');
  stage.addEventListener('mouseenter', () => { autoRotate = false; });
  stage.addEventListener('mouseleave', () => { autoRotate = true; lastTime = performance.now(); });

  // Drag to rotate. Single shared "release back to autorotate" timer so
  // rapid drag-release-drag cycles don't stack overlapping setTimeouts that
  // would all fire later and force-resume rotation while the user is still
  // interacting. pointercancel covers OS-level interruptions (touch loss,
  // window blur, gesture switch) where pointerup never arrives.
  let dragStart = null;
  let dragRot = null;
  let resumeTimer = null;
  function endDrag() {
    if (!dragStart) return;
    dragStart = null;
    if (resumeTimer) clearTimeout(resumeTimer);
    resumeTimer = setTimeout(() => {
      autoRotate = true;
      lastTime = performance.now();
      resumeTimer = null;
    }, 1500);
  }
  svg.on('pointerdown', (event) => {
    dragStart = [event.clientX, event.clientY];
    dragRot = [...rotation];
    autoRotate = false;
    if (resumeTimer) { clearTimeout(resumeTimer); resumeTimer = null; }
  });
  window.addEventListener('pointermove', (event) => {
    if (!dragStart) return;
    const dx = event.clientX - dragStart[0];
    const dy = event.clientY - dragStart[1];
    rotation[0] = dragRot[0] + dx * 0.35;
    rotation[1] = Math.max(-85, Math.min(85, dragRot[1] - dy * 0.35));
    projection.rotate(rotation);
    render();
  });
  window.addEventListener('pointerup', endDrag);
  window.addEventListener('pointercancel', endDrag);
  window.addEventListener('blur', endDrag);

  // Helper: build an arc geojson from a list of [lon,lat]
  function lineString(coords) {
    return { type: 'LineString', coordinates: coords };
  }

  // ── Temporal era → arc-risk mapping ────────────────────────────────────
  // The static arc.risk in data.js represents the CURRENT (war-era) state.
  // When the user scrubs the temporal filter back, we re-classify each arc
  // to reflect what the threat picture actually looked like at that date.
  function currentEra() {
    const t = window.CP && window.CP.timeline;
    return (t && t.era) || 'war';
  }
  function adjustedRisk(arc, era) {
    if (era === 'pre') {
      // No active maritime crisis in Oct–Nov 2023. Cape detours don't
      // exist yet (Suez+Hormuz both nominal); everything else is baseline.
      if (arc.risk === 'safe') return 'hidden';
      return 'flow';
    }
    if (era === 'houthi') {
      // Dec 2023 → Jan 2026: Houthi maritime campaign — Bab/Red Sea routes
      // become high-risk, Cape detours come online, Hormuz still nominal.
      if (arc.id === 'jeddah-rotter' || arc.id === 'basrah-rotter') return 'high';
      if (arc.risk === 'critical') return 'flow';   // Hormuz wasn't a war zone yet
      return arc.risk;                              // 'safe' (Cape) and 'flow' pass through
    }
    // 'war' / 'now': use original risk
    return arc.risk;
  }

  // Determine if a point is on the visible hemisphere
  function isVisible(lonLat) {
    const [lon, lat] = lonLat;
    const [rx, ry] = rotation;
    // great-circle distance from center of projection
    const λ = (lon + rx) * Math.PI/180;
    const φ1 = lat * Math.PI/180;
    const φ2 = -ry * Math.PI/180;
    const cos = Math.sin(φ1)*Math.sin(φ2) + Math.cos(φ1)*Math.cos(φ2)*Math.cos(λ);
    return cos > 0;
  }

  // Particles (animate along critical arcs)
  const particles = [];
  (window.ARCS || []).forEach(arc => {
    if (arc.risk === 'critical' || arc.risk === 'high') {
      const count = arc.risk === 'critical' ? 3 : 2;
      for (let i = 0; i < count; i++) {
        particles.push({ arc, t: i / count, speed: 0.00018 + Math.random()*0.0001 });
      }
    } else if (arc.risk === 'safe') {
      particles.push({ arc, t: Math.random(), speed: 0.0001 });
    }
  });

  function pointOnArc(arc, t) {
    const n = arc.path.length - 1;
    const f = t * n;
    const i = Math.floor(f);
    const u = f - i;
    const a = arc.path[Math.min(i, n)];
    const b = arc.path[Math.min(i+1, n)];
    return [ a[0] + (b[0]-a[0])*u, a[1] + (b[1]-a[1])*u ];
  }

  function animateParticles(now) {
    const era = currentEra();
    particles.forEach(p => {
      p.t += p.speed * 16;
      if (p.t >= 1) p.t = 0;
    });
    particleLayer.selectAll('circle')
      .data(particles)
      .join('circle')
      .each(function(p){
        const pt = pointOnArc(p.arc, p.t);
        const visible = isVisible(pt);
        const [x, y] = projection(pt);
        const sel = d3.select(this);
        const eraRisk = adjustedRisk(p.arc, era);
        // Hide particles for arcs that are not active in this era
        if (!visible || isNaN(x) || eraRisk === 'hidden') { sel.attr('r', 0); return; }
        const color = eraRisk === 'critical' ? '#ff3d5e'
                    : eraRisk === 'high'     ? '#ff8c42'
                    : eraRisk === 'safe'     ? '#00e690'
                    : '#00d4ff';
        sel.attr('cx', x).attr('cy', y).attr('r', eraRisk === 'critical' ? 2.6 : 2)
           .attr('fill', color)
           .attr('style', `color:${color}`)
           .attr('class', 'arc-particle');
      });
  }

  // Re-render arcs/particles when the temporal scrubber moves. The animation
  // tick will pick up the next state on its own, but we trigger an immediate
  // render() so the user sees the change without waiting for autoRotate.
  window.addEventListener('timeline-set', () => render());

  // Render (re-project everything that depends on rotation)
  function render(){
    // land + graticule + sphere
    svg.selectAll('.globe-sphere').attr('d', path);
    svg.selectAll('.globe-graticule').attr('d', path);
    landLayer.selectAll('.globe-land').attr('d', path);

    // Arcs — apply era-adjusted risk class, drop arcs that are 'hidden' for
    // the current era (e.g. Cape detours in pre-crisis era).
    const era = currentEra();
    const arcs = (window.ARCS || [])
      .map(a => ({ ...a, _era: adjustedRisk(a, era) }))
      .filter(a => a._era !== 'hidden');
    arcLayer.selectAll('path')
      .data(arcs, d => d.id)
      .join('path')
      .attr('class', d => `globe-arc arc-${d._era}`)
      .attr('d', d => path(lineString(d.path)));

    // Chokepoint + destination nodes. Iterate CHOKEPOINTS directly — the
    // earlier "skip suez then re-add" branch was a dead leftover from when
    // suez wasn't in the dict; the result was identical to a flat iteration.
    const nodeList = [];
    Object.values(window.CHOKEPOINTS || {}).forEach(cp => {
      nodeList.push({ ...cp, isChokepoint: true });
    });
    (window.NODES || []).forEach(n => nodeList.push({ ...n, isChokepoint: false }));

    const nodes = nodeLayer.selectAll('g.globe-node')
      .data(nodeList, d => d.id)
      .join(enter => {
        const g = enter.append('g').attr('class','globe-node');
        g.append('circle').attr('class','node-pulse');
        g.append('circle').attr('class','node-dot');
        g.append('text').attr('class','node-label');
        return g;
      });

    nodes.each(function(d){
      const sel = d3.select(this);
      const visible = isVisible([d.lon, d.lat]);
      if (!visible) { sel.attr('display','none'); return; }
      sel.attr('display', null);
      const [x, y] = projection([d.lon, d.lat]);
      const isCp = d.isChokepoint;
      const fill = d.threat === 'critical' ? '#ff3d5e'
                 : d.threat === 'high' ? '#ff8c42'
                 : d.threat === 'elevated' ? '#ffab00'
                 : d.threat === 'safe' ? '#00e690'
                 : '#00d4ff';
      sel.select('.node-dot')
        .attr('cx', x).attr('cy', y)
        .attr('r', isCp ? 5 : 3)
        .attr('fill', fill);
      if (isCp) {
        sel.select('.node-pulse')
          .attr('cx', x).attr('cy', y)
          .attr('stroke', fill)
          .attr('display', null);
      } else {
        sel.select('.node-pulse').attr('display','none');
      }
      sel.select('text')
        .attr('x', x + 8).attr('y', y + 3)
        .text(d.name || '');
    });

    // Click
    nodes.on('click', (event, d) => {
      if (!d.isChokepoint) return;
      window.dispatchEvent(new CustomEvent('chokepoint-select', { detail: d.id }));
    });
  }

})();
