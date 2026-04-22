/* ═══════════════════════════════════════════════════════════════════════════
   CHOKEPOINT INTEL — Static Geographic Primitives
   ───────────────────────────────────────────────────────────────────────────
   ONLY routing geography lives here. The Strait of Hormuz, Suez Canal, Bab
   el-Mandeb, Cape of Good Hope, and major port coordinates are physical
   constants — they do not change day to day. Everything else (threat levels,
   flow mbd, vessel counts, incident counts, prices, events, feeds) is loaded
   at runtime from the backend via window.API. See hydrate.js.
   ═══════════════════════════════════════════════════════════════════════════ */

// Physical chokepoint geography. Analytical fields (threat, flowMbd, vessels,
// incidents30d) are filled in at runtime by hydrate.js from the backend.
window.CHOKEPOINTS = {
  hormuz: {
    id: 'hormuz', name: 'Strait of Hormuz',
    lat: 26.57, lon: 56.25, widthMi: 21,
    threat: null, threatPct: null, flowMbd: null,
    vesselsInZone: null, incidents30d: null, routes: [], vessels: []
  },
  bab: {
    id: 'bab', name: 'Bab el-Mandeb',
    lat: 12.58, lon: 43.33, widthMi: 18,
    threat: null, threatPct: null, flowMbd: null,
    vesselsInZone: null, incidents30d: null, routes: [], vessels: []
  },
  cape: {
    id: 'cape', name: 'Cape of Good Hope',
    lat: -34.35, lon: 18.47, widthMi: null,
    threat: null, threatPct: null, flowMbd: null,
    vesselsInZone: null, incidents30d: null, routes: [], vessels: []
  },
  suez: {
    id: 'suez', name: 'Suez Canal',
    lat: 30.42, lon: 32.35, widthMi: 0.12,
    threat: null, threatPct: null, flowMbd: null,
    vesselsInZone: null, incidents30d: null, routes: [], vessels: []
  }
};

// Major export terminals and import ports — physical locations
window.NODES = [
  { id: 'ras',       name: 'Ras Tanura', lat: 26.64, lon: 50.17,  kind: 'export' },
  { id: 'kharg',     name: 'Kharg Is.',  lat: 29.23, lon: 50.32,  kind: 'export' },
  { id: 'basrah',    name: 'Basrah',     lat: 30.50, lon: 47.82,  kind: 'export' },
  { id: 'jeddah',    name: 'Jeddah',     lat: 21.49, lon: 39.19,  kind: 'port'   },
  { id: 'rotter',    name: 'Rotterdam',  lat: 51.95, lon:  4.14,  kind: 'import' },
  { id: 'singapore', name: 'Singapore',  lat:  1.26, lon: 103.83, kind: 'import' },
  { id: 'ningbo',    name: 'Ningbo',     lat: 29.87, lon: 121.54, kind: 'import' },
  { id: 'ulsan',     name: 'Ulsan',      lat: 35.49, lon: 129.36, kind: 'import' },
  { id: 'houston',   name: 'Houston',    lat: 29.72, lon: -95.28, kind: 'import' },
  { id: 'mumbai',    name: 'Mumbai',     lat: 19.07, lon:  72.87, kind: 'import' }
];

// Shipping arcs — waypoints hand-tuned to stay over water. Geographic only;
// risk is recomputed at runtime from live chokepoint status.
//
// Routing notes (do not edit a path without verifying against a real map):
//   • Red Sea legs follow the central deep trough; the Saudi west coast sits
//     near 41-42°E and the Eritrean/Yemen coast near 41°E, so a single offset
//     of 0.5-1° picks the wrong country.
//   • Hormuz → Bab el-Mandeb routes drop south through the Arabian Sea
//     (lat ~12°N) before turning west, never cutting across mainland Yemen.
//   • East Asia routes from the Strait of Malacca stay east of Vietnam (≥109°E),
//     east of Hainan (Hainan = 108-111°E, 18-20°N), and run through the
//     Taiwan Strait centerline (~119-121°E) — never over Taiwan or the
//     Pescadores.
//   • Cape of Good Hope routes pass east of Madagascar (Madagascar = 43-50°E,
//     12-26°S), then south of South Africa at ≥35°S before turning into the
//     Atlantic. They never cross the Mozambique Channel.
window.ARCS = [
  // ═══ Arabian Gulf outflow via Strait of Hormuz ═══
  { id: 'ras-sing', from: 'ras', to: 'singapore', risk: 'critical',
    path: [[50.17,26.64],[52,26.5],[55,26.3],[56.3,26.5],[58,25],[60,23],[62,20],[65,15],[70,10],[78,7],[88,5],[95,4],[100,3.2],[102.5,2],[103.83,1.26]] },
  { id: 'kharg-ningbo', from: 'kharg', to: 'ningbo', risk: 'critical',
    path: [[50.32,29.23],[51,28.5],[53,27.5],[55,26.8],[56.3,26.5],[58,25],[60,22],[64,17],[72,10],[82,6],[92,5],[100,3.5],[103.83,1.26],[107,5],[111,10],[114,15],[117,19],[119,22],[120,25],[121,28],[121.54,29.87]] },
  { id: 'basrah-rotter', from: 'basrah', to: 'rotter', risk: 'critical',
    path: [[47.82,30.5],[49,29.5],[51,28],[54,26.8],[56.3,26.5],[58,24.5],[59,21],[59,15],[57,12],[52,12],[48,12.3],[45,12.4],[43.3,12.6],[42.2,14],[41.3,16],[40.3,18],[39.3,20],[38.2,22],[37,24],[35.5,26],[34.2,27.8],[33,29.3],[32.55,29.95],[32.35,30.42],[32.3,31.3],[31,32],[25,33.5],[15,36],[6,37.5],[-1,36],[-5.5,36],[-9,37],[-10,44],[-6,48],[0,50],[4.14,51.95]] },
  { id: 'kuwait-ulsan', from: 'basrah', to: 'ulsan', risk: 'critical',
    path: [[47.82,30.5],[49,29],[52,27.5],[55,26.8],[56.3,26.5],[58,24],[60,20],[65,13],[75,8],[88,5],[98,3.5],[103.83,1.26],[107,4],[111,9],[114,14],[117,18],[120,21.5],[122.5,24],[124,27],[126,30],[128,33],[129.36,35.49]] },

  // ═══ Red Sea via Bab el-Mandeb → Suez → Europe ═══
  { id: 'jeddah-rotter', from: 'jeddah', to: 'rotter', risk: 'high',
    path: [[39.19,21.49],[37.8,23],[36.5,25],[35,26.8],[33.8,28.2],[32.8,29.3],[32.55,29.95],[32.35,30.42],[32.3,31.3],[31,32],[25,33.5],[15,36],[6,37.5],[-1,36],[-5.5,36],[-9,37],[-10,44],[-6,48],[0,50],[4.14,51.95]] },

  // ═══ Cape of Good Hope alternative routes ═══
  { id: 'ras-houston-cape', from: 'ras', to: 'houston', risk: 'safe',
    path: [[50.17,26.64],[52,26.5],[55,26.3],[56.3,26.5],[58,24.5],[60,21],[62,15],[62,8],[58,0],[55,-10],[53,-20],[51,-28],[42,-35],[30,-37],[22,-36],[18.47,-34.35],[10,-34],[0,-32],[-10,-27],[-20,-20],[-30,-12],[-42,-2],[-55,8],[-68,15],[-80,20],[-88,23],[-93,27],[-95.28,29.72]] },
  { id: 'ras-rotter-cape', from: 'ras', to: 'rotter', risk: 'safe',
    path: [[50.17,26.64],[52,26.5],[55,26.3],[56.3,26.5],[58,24],[60,19],[62,12],[60,0],[58,-12],[55,-22],[52,-30],[42,-35],[30,-37],[22,-36],[18.47,-34.35],[12,-30],[8,-20],[5,-10],[-2,0],[-10,10],[-17,20],[-18,30],[-12,38],[-5,44],[0,49],[4.14,51.95]] },

  // ═══ Baseline regional flow ═══
  { id: 'ras-mumbai', from: 'ras', to: 'mumbai', risk: 'flow',
    path: [[50.17,26.64],[52,26.5],[55,26.3],[56.3,26.5],[58,24.5],[60,23],[63,21],[67,19.5],[71,19],[72.87,19.07]] },
  { id: 'jeddah-sing', from: 'jeddah', to: 'singapore', risk: 'flow',
    path: [[39.19,21.49],[40.5,18.5],[41.5,16],[42.2,14],[43.3,12.6],[45,12.5],[48,12.5],[52,12.5],[58,10],[65,8],[75,7],[85,5],[95,4],[100,3.2],[103.83,1.26]] }
];

// Runtime state — hydrated from backend. NO hardcoded values here.
window.ATTACKS       = [];  // from /api/events (ACLED)
window.BRENT_SPARK   = [];  // from /api/brent
window.IRAN_EVENTS   = [];  // from /api/iran-events (curated)
window.FEED          = [];  // from /api/iran-events (news)
window.THESIS_EVENTS = [];  // from /api/thesis-events (726 ACLED historical)
window.MASTER        = null; // from /api/master
