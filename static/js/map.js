/* ═══════════════════════════════════════════════════════════════════════════
   Interactive Map — Leaflet.js with heatmap, clustering, time slider
   ═══════════════════════════════════════════════════════════════════════════ */

let map = null;
let heatLayer = null;
let markerClusterGroup = null;
let chokepointRect = null;
let allMapEvents = [];
let filteredMapEvents = [];
let timeSlider = null;
let mapInitialized = false;

const TANKER_KEYWORDS = /tanker|crude|oil|petroleum|lng|lpg|chemical/i;
const CHOKEPOINT_LAT_MIN = 12.4;
const CHOKEPOINT_LAT_MAX = 13.8;
const MAP_CENTER = [14.0, 44.0];
const MAP_ZOOM = 6;

function isTankerTarget(notes) {
    return TANKER_KEYWORDS.test(notes || '');
}

function isChokepoint(lat) {
    return lat >= CHOKEPOINT_LAT_MIN && lat <= CHOKEPOINT_LAT_MAX;
}

function initMap() {
    if (mapInitialized) return;
    mapInitialized = true;

    map = L.map('map', {
        center: MAP_CENTER,
        zoom: MAP_ZOOM,
        zoomControl: true,
        attributionControl: true,
    });

    // Dark tiles — CartoDB Dark Matter (free, no key)
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; <a href="https://carto.com/">CARTO</a>',
        subdomains: 'abcd',
        maxZoom: 19,
    }).addTo(map);

    // Initialize empty layers
    heatLayer = L.heatLayer([], {
        radius: 25,
        blur: 15,
        maxZoom: 10,
        gradient: { 0.2: '#3D6B99', 0.4: '#D4A843', 0.6: '#E07B4C', 0.8: '#C43D3D', 1.0: '#8B0000' }
    }).addTo(map);

    markerClusterGroup = L.markerClusterGroup({
        maxClusterRadius: 40,
        iconCreateFunction: function(cluster) {
            const count = cluster.getChildCount();
            let size = 'small';
            if (count > 20) size = 'large';
            else if (count > 10) size = 'medium';
            return L.divIcon({
                html: `<div class="cluster-icon cluster-${size}">${count}</div>`,
                className: 'custom-cluster',
                iconSize: [36, 36],
            });
        }
    }).addTo(map);

    // Chokepoint zone rectangle
    chokepointRect = L.rectangle(
        [[CHOKEPOINT_LAT_MIN, 41.5], [CHOKEPOINT_LAT_MAX, 45.5]],
        {
            color: '#D4A843',
            weight: 2,
            dashArray: '6 4',
            fillColor: '#D4A843',
            fillOpacity: 0.08,
        }
    ).addTo(map);

    // Add label for chokepoint
    L.marker([12.6, 43.4], {
        icon: L.divIcon({
            className: 'chokepoint-label',
            html: '<div style="background:rgba(27,42,74,0.9);color:#C9A96E;padding:4px 8px;border-radius:4px;font-size:11px;font-weight:600;white-space:nowrap;font-family:Inter,sans-serif">Bab el-Mandeb Strait</div>',
            iconSize: [140, 24],
            iconAnchor: [70, 12],
        })
    }).addTo(map);

    // Add cluster CSS
    const style = document.createElement('style');
    style.textContent = `
        .custom-cluster { background: none !important; border: none !important; }
        .cluster-icon {
            display: flex; align-items: center; justify-content: center;
            border-radius: 50%; color: white; font-weight: 700;
            font-family: Inter, sans-serif; font-size: 12px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.3);
        }
        .cluster-small { background: rgba(61,107,153,0.85); width: 30px; height: 30px; }
        .cluster-medium { background: rgba(212,168,67,0.85); width: 36px; height: 36px; }
        .cluster-large { background: rgba(196,61,61,0.85); width: 42px; height: 42px; font-size: 14px; }
    `;
    document.head.appendChild(style);

    // Setup controls
    setupMapControls();
}

function loadMapEvents(events) {
    allMapEvents = events.filter(e => e.latitude && e.longitude).map(e => ({
        ...e,
        lat: parseFloat(e.latitude),
        lng: parseFloat(e.longitude),
        isTanker: isTankerTarget(e.notes),
        isChokepoint: isChokepoint(parseFloat(e.latitude)),
        dateObj: new Date(e.event_date),
        timestamp: new Date(e.event_date).getTime(),
        fatalities: parseInt(e.fatalities) || 0,
    }));

    filteredMapEvents = [...allMapEvents];
    setupTimeSlider();
    updateMapLayers();
    updateMapStats();
}

function updateMapLayers() {
    if (!map) return;

    const showHeatmap = document.getElementById('toggleHeatmap')?.checked ?? true;
    const showMarkers = document.getElementById('toggleMarkers')?.checked ?? true;
    const showChokepoint = document.getElementById('toggleChokepoint')?.checked ?? true;
    const tankerOnly = document.getElementById('filterTankers')?.checked ?? false;

    let events = filteredMapEvents;
    if (tankerOnly) {
        events = events.filter(e => e.isTanker);
    }

    // Update heatmap
    if (showHeatmap && heatLayer) {
        const heatData = events.map(e => [e.lat, e.lng, 0.5]);
        heatLayer.setLatLngs(heatData);
        if (!map.hasLayer(heatLayer)) map.addLayer(heatLayer);
    } else if (heatLayer) {
        map.removeLayer(heatLayer);
    }

    // Update markers
    markerClusterGroup.clearLayers();
    if (showMarkers) {
        events.forEach(e => {
            const color = e.isTanker ? '#C43D3D' : '#3D6B99';
            const size = e.isTanker ? 8 : 6;

            const marker = L.circleMarker([e.lat, e.lng], {
                radius: size,
                fillColor: color,
                color: '#fff',
                weight: 1,
                fillOpacity: 0.8,
            });

            const dateStr = e.event_date ? e.event_date.substring(0, 10) : 'Unknown';
            const notesShort = (e.notes || '').substring(0, 200);

            marker.bindPopup(`
                <div style="font-family:Inter,sans-serif;max-width:300px">
                    <div style="font-weight:700;font-size:13px;margin-bottom:6px;color:#1B2A4A">${dateStr}</div>
                    <div style="font-size:12px;margin-bottom:4px">
                        <span style="color:#636E72">Type:</span> ${e.event_type || 'N/A'}
                    </div>
                    <div style="font-size:12px;margin-bottom:4px">
                        <span style="color:#636E72">Sub-type:</span> ${e.sub_event_type || 'N/A'}
                    </div>
                    <div style="font-size:12px;margin-bottom:4px">
                        <span style="color:#636E72">Actor:</span> ${e.actor1 || 'N/A'}
                    </div>
                    <div style="font-size:12px;margin-bottom:4px">
                        <span style="color:#636E72">Location:</span> ${e.location || 'N/A'}
                    </div>
                    <div style="font-size:12px;margin-bottom:4px">
                        <span style="color:#636E72">Fatalities:</span> ${e.fatalities}
                    </div>
                    ${e.isTanker ? '<div style="font-size:11px;color:#C43D3D;font-weight:600;margin-top:6px">⚠ TANKER/ENERGY TARGET</div>' : ''}
                    <div style="font-size:11px;color:#636E72;margin-top:8px;line-height:1.4">${notesShort}${(e.notes || '').length > 200 ? '...' : ''}</div>
                </div>
            `, { maxWidth: 320 });

            markerClusterGroup.addLayer(marker);
        });
    }

    // Chokepoint zone
    if (chokepointRect) {
        if (showChokepoint) {
            if (!map.hasLayer(chokepointRect)) map.addLayer(chokepointRect);
        } else {
            map.removeLayer(chokepointRect);
        }
    }
}

function updateMapStats() {
    const tankerOnly = document.getElementById('filterTankers')?.checked ?? false;
    let events = filteredMapEvents;
    if (tankerOnly) events = events.filter(e => e.isTanker);

    const el = (id, val) => {
        const elem = document.getElementById(id);
        if (elem) elem.textContent = val;
    };

    el('stat-total', events.length);
    el('stat-tanker', events.filter(e => e.isTanker).length);
    el('stat-chokepoint', events.filter(e => e.isChokepoint).length);
    el('stat-fatalities', events.reduce((sum, e) => sum + e.fatalities, 0));
}

function setupTimeSlider() {
    if (!allMapEvents.length) return;

    const slider = document.getElementById('timeSlider');
    if (!slider) return;

    const timestamps = allMapEvents.map(e => e.timestamp);
    const minTs = Math.min(...timestamps);
    const maxTs = Math.max(...timestamps);

    // Destroy existing slider
    if (slider.noUiSlider) slider.noUiSlider.destroy();

    noUiSlider.create(slider, {
        start: [minTs, maxTs],
        connect: true,
        range: { min: minTs, max: maxTs },
        step: 86400000, // 1 day
    });

    const formatDate = ts => {
        const d = new Date(ts);
        return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
    };

    document.getElementById('sliderStartDate').textContent = formatDate(minTs);
    document.getElementById('sliderEndDate').textContent = formatDate(maxTs);

    slider.noUiSlider.on('update', function(values) {
        const [start, end] = values.map(Number);
        document.getElementById('sliderStartDate').textContent = formatDate(start);
        document.getElementById('sliderEndDate').textContent = formatDate(end);

        filteredMapEvents = allMapEvents.filter(e => e.timestamp >= start && e.timestamp <= end);
        updateMapLayers();
        updateMapStats();
    });

    timeSlider = slider;
}

function setupMapControls() {
    ['toggleHeatmap', 'toggleMarkers', 'toggleChokepoint', 'filterTankers'].forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            el.addEventListener('change', () => {
                updateMapLayers();
                updateMapStats();
            });
        }
    });
}

function resizeMap() {
    if (map) {
        setTimeout(() => map.invalidateSize(), 100);
    }
}
