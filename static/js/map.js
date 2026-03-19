/* ═══════════════════════════════════════════════════════════════════════════
   Interactive Map — Leaflet.js with heatmap, clustering, time slider
   ═══════════════════════════════════════════════════════════════════════════ */

// HTML escape helper (XSS prevention for map popups)
function _escMap(str) {
    if (str == null) return '';
    const div = document.createElement('div');
    div.textContent = String(str);
    return div.innerHTML;
}

let map = null;
let heatLayer = null;
let markerClusterGroup = null;
let chokepointRect = null;
let allMapEvents = [];
let filteredMapEvents = [];
let timeSlider = null;
let mapInitialized = false;

// Maritime vessel keywords — must indicate an actual ship, not land-based "oil facility" or "water tanker"
const VESSEL_CONTEXT = /\b(vessel|ship|carrier|MV |MT |IMO:|flag(?:ged)?)\b/i;
const ENERGY_CARGO = /\b(crude|petroleum|lng|lpg)\b/i;
const MARITIME_TANKER = /\b(?:oil[- ]?tanker|chemical[- ]?tanker|fuel[- ]?tanker|products?\s+tanker)\b(?!\s+(?:truck|lorry|driver))/i;
const MARITIME_LOCATION = /red sea|gulf of aden|bab el.mandeb|strait of hormuz|arabian sea|indian ocean|south red sea/i;
// Filter to only maritime/shipping-relevant events (exclude land-based Yemen civil conflict)
const MARITIME_EVENT = /\b(vessel|ship|tanker|maritime|navy|destroyer|frigate|warship|anti-ship|drone.?boat|hijack|boarding|piracy|naval|USS |HMS |USNS |MV |MT |IMO:|missile.*launch|intercept(?:ed|ion)|shot down|drone|ballistic|cruise missile|one-way attack|houthi.*fire|fire.*houthi|sea mine)\b/i;
const MARITIME_LOC = /red sea|gulf of aden|bab el.mandeb|south red sea|north red sea|west arabian/i;

// Strict ship/vessel keywords — the thesis counts attacks on vessels, not land targets
const SHIP_KEYWORDS = /\b(vessel|ship|tanker|cargo|bulk.?carrier|container.?ship|MV |MT |HMS |USS |USNS |warship|destroyer|frigate|corvette|naval|navy|maritime|hijack|boarding|piracy|sea.?mine)\b/i;

function isMaritimeRelevant(event) {
    const loc = event.location || '';
    const notes = event.notes || '';
    const lat = parseFloat(event.latitude);
    const lon = parseFloat(event.longitude);

    // Geographic bounds: Red Sea / Gulf of Aden / Bab el-Mandeb / Arabian Sea theater
    // Two zones: Southern (Gulf of Aden + Arabian Sea) and Northern (Red Sea up to Suez)
    // Excludes Persian Gulf (lat>20 AND lon>45), Strait of Hormuz, Mediterranean
    const hasCoords = !isNaN(lat) && !isNaN(lon);
    const inSouthernZone = hasCoords && lat >= 10.0 && lat <= 20.0 && lon >= 36.0 && lon <= 62.0;
    const inNorthernRedSea = hasCoords && lat > 20.0 && lat <= 30.5 && lon >= 32.0 && lon <= 44.0;
    const inGeoBounds = inSouthernZone || inNorthernRedSea;

    // Must involve an actual ship/vessel
    const involvesShip = SHIP_KEYWORDS.test(notes);

    // Exclude protests — not part of thesis analysis
    const eventType = (event.event_type || '').toLowerCase();
    if (eventType === 'protests') return false;

    // Geographic bounds required (text-only matching too error-prone for out-of-theater events)
    return inGeoBounds && involvesShip;
}

const CHOKEPOINT_LAT_MIN = 12.4;
const CHOKEPOINT_LAT_MAX = 13.8;
const MAP_CENTER = [14.0, 44.0];
const MAP_ZOOM = 6;

function isTankerTarget(notes) {
    if (!notes) return false;
    // Direct match: "oil tanker", "chemical tanker", etc.
    if (MARITIME_TANKER.test(notes)) return true;
    // "tanker" + maritime location context (not land-based water/fuel tankers)
    if (/\btanker\b/i.test(notes) && MARITIME_LOCATION.test(notes)) return true;
    // Energy cargo (crude, LNG, LPG) + vessel context (ship, vessel, MV, IMO)
    if (ENERGY_CARGO.test(notes) && VESSEL_CONTEXT.test(notes)) return true;
    return false;
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
        gradient: { 0.1: '#0a2463', 0.3: '#00d4ff33', 0.5: '#00d4ff', 0.7: '#ffcc00', 0.85: '#ff4444', 1.0: '#ff0000' }
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
            color: '#00d4ff',
            weight: 1.5,
            dashArray: '6 4',
            fillColor: '#00d4ff',
            fillOpacity: 0.04,
        }
    ).addTo(map);

    // Add label for chokepoint
    L.marker([12.6, 43.4], {
        icon: L.divIcon({
            className: 'chokepoint-label',
            html: '<div style="background:rgba(7,13,21,0.85);color:#00d4ff;padding:2px 6px;border:1px solid rgba(0,212,255,0.2);border-radius:2px;font-size:9px;font-weight:700;white-space:nowrap;font-family:\'SF Mono\',\'Fira Code\',Consolas,monospace;letter-spacing:1.5px">BAB EL-MANDEB STRAIT</div>',
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
        .cluster-small { background: rgba(0,212,255,0.7); width: 30px; height: 30px; box-shadow: 0 0 8px rgba(0,212,255,0.4); }
        .cluster-medium { background: rgba(255,204,0,0.7); width: 36px; height: 36px; box-shadow: 0 0 8px rgba(255,204,0,0.4); }
        .cluster-large { background: rgba(255,68,68,0.7); width: 42px; height: 42px; font-size: 14px; box-shadow: 0 0 10px rgba(255,68,68,0.4); }
    `;
    document.head.appendChild(style);

    // Setup controls
    setupMapControls();
}

function loadMapEvents(events, skipFilter = false) {
    allMapEvents = events.filter(e => e.latitude && e.longitude && (skipFilter || isMaritimeRelevant(e))).map(e => ({
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
                color: color,
                weight: 1.5,
                fillOpacity: 0.55,
                opacity: 0.8,
            });

            // Glow ring for tanker targets
            if (e.isTanker) {
                const glow = L.circleMarker([e.lat, e.lng], {
                    radius: size + 4,
                    fillColor: color,
                    color: color,
                    weight: 0,
                    fillOpacity: 0.15,
                    interactive: false,
                });
                markerClusterGroup.addLayer(glow);
            }

            const dateStr = e.event_date ? e.event_date.substring(0, 10) : 'Unknown';
            const notesShort = (e.notes || '').substring(0, 200);

            const h = _escMap;
            marker.bindPopup(`
                <div style="font-family:'SF Mono','Fira Code',Consolas,monospace;max-width:300px">
                    <div style="font-weight:700;font-size:11px;margin-bottom:6px;color:#00d4ff;letter-spacing:1px">${h(dateStr)}</div>
                    <div style="font-size:10px;margin-bottom:3px">
                        <span style="color:#5a6a7a">TYPE</span> <span style="color:#c0cad8;margin-left:4px">${h(e.event_type || 'N/A')}</span>
                    </div>
                    <div style="font-size:10px;margin-bottom:3px">
                        <span style="color:#5a6a7a">SUB</span> <span style="color:#c0cad8;margin-left:4px">${h(e.sub_event_type || 'N/A')}</span>
                    </div>
                    <div style="font-size:10px;margin-bottom:3px">
                        <span style="color:#5a6a7a">ACTOR</span> <span style="color:#c0cad8;margin-left:4px">${h(e.actor1 || 'N/A')}</span>
                    </div>
                    <div style="font-size:10px;margin-bottom:3px">
                        <span style="color:#5a6a7a">LOC</span> <span style="color:#c0cad8;margin-left:4px">${h(e.location || 'N/A')}</span>
                    </div>
                    <div style="font-size:10px;margin-bottom:3px">
                        <span style="color:#5a6a7a">KIA</span> <strong style="color:${e.fatalities > 0 ? '#ff4444' : '#c0cad8'};margin-left:4px">${h(e.fatalities)}</strong>
                    </div>
                    ${e.isTanker ? '<div style="font-size:10px;color:#ff4444;font-weight:700;margin-top:6px;letter-spacing:0.5px">TANKER / ENERGY TARGET</div>' : ''}
                    <div style="font-size:9px;color:#4a5a6a;margin-top:8px;line-height:1.4">${h(notesShort)}${(e.notes || '').length > 200 ? '...' : ''}</div>
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
