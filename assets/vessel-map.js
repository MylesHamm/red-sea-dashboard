/* Self-hosted chokepoint vessel maps — Leaflet-based.
 *
 * Replaces the MarineTraffic iframe whose /ais/embed/ endpoint silently drops
 * vessel-type filters (`vtypes:` is only honored on /ais/home/, which CSP
 * `frame-ancestors` blocks from being iframed). With a self-hosted map the
 * filter chips actually filter the visible map, not just the "open in new
 * tab" link.
 *
 * Data source: /api/vessels/{chokepoint} — live AISStream.io feed, with a
 * representative-tanker fallback when the live feed is unavailable. Each
 * vessel payload carries `representative: true` when it came from the
 * fallback, so the map shows a clear banner in that case.
 *
 * Tiles: CARTO dark-matter (free, no key, matches dashboard palette).
 *
 * Each .vt-map element declares its config via data-* attributes:
 *   data-vm-cp="hormuz"        — which API path to poll
 *   data-vm-center="lat,lon"   — initial map center
 *   data-vm-zoom="N"           — initial zoom level
 *   data-vm-ring-km="N"        — chokepoint-radius ring in km
 */
(function () {
  'use strict';

  if (typeof L === 'undefined') {
    console.warn('[vessel-map] Leaflet not loaded — vessel maps disabled');
    return;
  }

  const POLL_INTERVAL_MS = 30_000;
  const TYPE_COLORS = {
    TANKER:    '#ff8c42',
    CARGO:     '#00d4ff',
    PASSENGER: '#c58bff',
    FISHING:   '#9aa5b8',
    HSC:       '#ffd166',
    SPECIAL:   '#ef476f',
    OTHER:     '#6b7688',
  };

  const maps = []; // {cp, map, markers: Map<mmsi, marker>, filterTypes: Set|'ALL'}

  function init() {
    document.querySelectorAll('.vt-map[data-vm-cp]').forEach(mountMap);
    document.querySelectorAll('.vt-filter[data-vm-filter]').forEach(wireFilter);
    // Kick off the first poll immediately + every POLL_INTERVAL_MS
    refreshAll();
    setInterval(refreshAll, POLL_INTERVAL_MS);
  }

  function mountMap(el) {
    const cp       = el.dataset.vmCp;
    const [lat, lon] = (el.dataset.vmCenter || '0,0').split(',').map(Number);
    const zoom     = parseInt(el.dataset.vmZoom, 10) || 9;
    const ringKm   = parseFloat(el.dataset.vmRingKm) || 15;

    const map = L.map(el, {
      zoomControl: true,
      attributionControl: true,
      scrollWheelZoom: false,  // avoid hijacking page scroll
    }).setView([lat, lon], zoom);

    // CARTO dark-matter tiles — free, matches the dashboard's dark palette.
    L.tileLayer(
      'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
      {
        attribution: '© OpenStreetMap · © CARTO',
        subdomains: 'abcd',
        maxZoom: 19,
      }
    ).addTo(map);

    // Chokepoint centerpoint marker + threat ring
    L.circleMarker([lat, lon], {
      radius: 4,
      color: '#ff3d5e',
      weight: 2,
      fillColor: '#ff3d5e',
      fillOpacity: 1,
    }).addTo(map).bindTooltip('Chokepoint center');

    L.circle([lat, lon], {
      radius: ringKm * 1000,
      color: '#ff3d5e',
      weight: 1,
      opacity: 0.5,
      fillColor: '#ff3d5e',
      fillOpacity: 0.06,
      dashArray: '4,6',
      interactive: false,
    }).addTo(map);

    const state = {
      cp, map,
      markers: new Map(),     // mmsi → leaflet marker
      filterTypes: new Set(['TANKER']),  // default: tankers only
      allVessels: [],
    };
    maps.push(state);
  }

  function wireFilter(filterEl) {
    const cp = filterEl.dataset.vmFilter;
    const chips = Array.from(filterEl.querySelectorAll('.vt-chip'));
    chips.forEach(chip => {
      chip.addEventListener('click', () => {
        chips.forEach(c => c.classList.remove('is-active'));
        chip.classList.add('is-active');
        const spec = (chip.dataset.vmTypes || 'ALL').trim();
        const state = maps.find(s => s.cp === cp);
        if (!state) return;
        state.filterTypes = spec === 'ALL' ? 'ALL' : new Set(spec.split(',').map(t => t.trim()));
        renderMarkers(state);
        updateCount(state);
      });
    });
  }

  async function refreshAll() {
    if (!window.API) return;
    let status = null;
    try { status = await window.API.vesselsStatus(); } catch (e) { /* fall through */ }

    await Promise.all(maps.map(async (state) => {
      try {
        const resp = await window.API.vessels(state.cp);
        const vessels = (resp && resp.data) || [];
        state.allVessels = vessels;
        renderMarkers(state);
        updateCount(state);
        updateBanner(state, status, vessels);
      } catch (e) {
        console.warn('[vessel-map] refresh failed:', state.cp, e);
        updateBanner(state, status, state.allVessels || []);
      }
    }));
  }

  function renderMarkers(state) {
    const shown = new Set();
    const vessels = (state.allVessels || []).filter(v => matchesFilter(v, state.filterTypes));

    vessels.forEach(v => {
      if (v.lat == null || v.lon == null) return;
      const key = String(v.mmsi);
      shown.add(key);
      const existing = state.markers.get(key);
      if (existing) {
        existing.setLatLng([v.lat, v.lon]);
        existing.setTooltipContent(tooltipFor(v));
        existing.setStyle({ fillColor: colorFor(v), color: colorFor(v) });
      } else {
        const marker = L.circleMarker([v.lat, v.lon], {
          radius: v.representative ? 5 : 6,
          color: colorFor(v),
          weight: 1.5,
          fillColor: colorFor(v),
          fillOpacity: v.representative ? 0.35 : 0.7,
          className: v.representative ? 'vt-marker-representative' : 'vt-marker-live',
        });
        marker.bindTooltip(tooltipFor(v), { direction: 'top', offset: [0, -4] });
        marker.addTo(state.map);
        state.markers.set(key, marker);
      }
    });

    // Remove markers that no longer match
    for (const [key, marker] of state.markers) {
      if (!shown.has(key)) {
        state.map.removeLayer(marker);
        state.markers.delete(key);
      }
    }
  }

  function updateCount(state) {
    const countEl = document.querySelector(`[data-vm-count="${state.cp}"]`);
    if (!countEl) return;
    const n = (state.allVessels || []).filter(v => matchesFilter(v, state.filterTypes)).length;
    countEl.textContent = n;
  }

  function updateBanner(state, status, vessels) {
    const bannerEl = document.querySelector(`[data-vm-banner="${state.cp}"]`);
    if (!bannerEl) return;
    const hasRepresentative = vessels.some(v => v.representative);
    const hasLive = vessels.some(v => !v.representative);

    let msg = '';
    let cls = '';
    if (status) {
      const err = (status.last_error || '').toLowerCase();
      const isRateLimited = err.includes('429') || err.includes('too many') || (err.includes('rate') && err.includes('limit'));
      const etaSec = status.retry_eta_ts ? Math.max(0, Math.round(status.retry_eta_ts - Date.now()/1000)) : null;
      const etaTxt = etaSec != null ? ` · next retry ${etaSec < 60 ? etaSec + 's' : Math.round(etaSec/60) + 'm'}` : '';

      if (!status.configured) {
        msg = 'AISStream.io API key not configured — showing representative tanker positions.';
        cls = 'vt-banner-warn';
      } else if (hasLive) {
        // live data is flowing — no banner needed
        msg = '';
      } else if (hasRepresentative && isRateLimited) {
        msg = `Live AIS feed rate-limited by provider${etaTxt} — showing representative tanker positions from established shipping lanes.`;
        cls = 'vt-banner-warn';
      } else if (hasRepresentative && !status.connected) {
        msg = `AIS feed reconnecting${etaTxt} — showing representative tanker positions.`;
        cls = 'vt-banner-warn';
      } else if (hasRepresentative) {
        msg = 'AIS feed connecting — showing representative tanker positions until first live broadcast.';
        cls = 'vt-banner-info';
      } else if (!vessels.length && !status.connected) {
        msg = `AIS feed unavailable${etaTxt}.`;
        cls = 'vt-banner-err';
      }
    }

    if (msg) {
      bannerEl.textContent = msg;
      bannerEl.className = 'vt-map-banner ' + cls;
      bannerEl.hidden = false;
    } else {
      bannerEl.hidden = true;
    }

    const statusEl = document.querySelector(`[data-vm-status="${state.cp}"]`);
    if (statusEl) {
      const dot = statusEl.querySelector('.vt-live-dot');
      if (hasLive) {
        statusEl.firstChild && (statusEl.lastChild.textContent = 'LIVE · AIS');
        statusEl.classList.remove('vt-live-fallback');
        if (dot) dot.style.background = '';
      } else if (hasRepresentative) {
        // Replace the text node after the dot
        statusEl.innerHTML = '<span class="vt-live-dot"></span>REPRESENTATIVE';
        statusEl.classList.add('vt-live-fallback');
      }
    }
  }

  function matchesFilter(v, filterTypes) {
    if (filterTypes === 'ALL') return true;
    return filterTypes.has((v.type || 'OTHER').toUpperCase());
  }

  function colorFor(v) {
    return TYPE_COLORS[(v.type || 'OTHER').toUpperCase()] || TYPE_COLORS.OTHER;
  }

  function tooltipFor(v) {
    const lines = [
      `<b>${escapeHtml(v.name || ('MMSI ' + v.mmsi))}</b>`,
      `${escapeHtml(v.type || 'OTHER')}${v.representative ? ' · representative' : ''}`,
      (v.speed != null ? `${v.speed.toFixed(1)} kt` : '— kt') +
        (v.course != null ? ` · ${Math.round(v.course)}°` : ''),
    ];
    return lines.join('<br>');
  }

  function escapeHtml(s) {
    return String(s == null ? '' : s).replace(/[&<>"]/g, c => (
      { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]
    ));
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
