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
    vesselsInZone: null, incidents30d: null,
    // Real Hormuz transit corridors — EIA Persian Gulf flow allocation
    routes: [
      { from: 'Ras Tanura',   to: 'Singapore',  mbd: 8.4, risk: 'critical' },
      { from: 'Kharg Island', to: 'China',      mbd: 1.6, risk: 'critical' },
      { from: 'Basrah',       to: 'Rotterdam',  mbd: 3.1, risk: 'critical' },
      { from: 'Kuwait',       to: 'S. Korea',   mbd: 2.4, risk: 'critical' }
    ],
    vessels: []
  },
  bab: {
    id: 'bab', name: 'Bab el-Mandeb',
    lat: 12.58, lon: 43.33, widthMi: 18,
    threat: null, threatPct: null, flowMbd: null,
    vesselsInZone: null, incidents30d: null,
    routes: [
      { from: 'Jeddah',     to: 'Rotterdam', mbd: 2.3, risk: 'high' },
      { from: 'Basrah',     to: 'Rotterdam', mbd: 3.1, risk: 'high' },
      { from: 'Yanbu',      to: 'Suez',      mbd: 1.8, risk: 'high' },
      { from: 'Aden',       to: 'Mumbai',    mbd: 1.0, risk: 'elevated' }
    ],
    vessels: []
  },
  cape: {
    id: 'cape', name: 'Cape of Good Hope',
    lat: -34.35, lon: 18.47, widthMi: null,
    threat: null, threatPct: null, flowMbd: null,
    vesselsInZone: null, incidents30d: null,
    routes: [
      { from: 'Ras Tanura', to: 'Rotterdam (Cape)', mbd: 2.1, risk: 'safe' },
      { from: 'Houston',    to: 'Singapore (Cape)', mbd: 1.4, risk: 'safe' },
      { from: 'Basrah',     to: 'Rotterdam (Cape)', mbd: 1.0, risk: 'safe' }
    ],
    vessels: []
  },
  suez: {
    id: 'suez', name: 'Suez Canal',
    lat: 30.42, lon: 32.35, widthMi: 0.12,
    threat: null, threatPct: null, flowMbd: null,
    vesselsInZone: null, incidents30d: null,
    routes: [
      { from: 'Ras Tanura', to: 'Rotterdam', mbd: 4.2, risk: 'elevated' },
      { from: 'Yanbu',      to: 'Rotterdam', mbd: 1.5, risk: 'elevated' },
      { from: 'Basrah',     to: 'Rotterdam', mbd: 2.4, risk: 'elevated' }
    ],
    vessels: []
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
// Routing notes (do NOT edit a path without verifying against a real map):
//   • Suez approach: routes from the Red Sea must round the SOUTHERN tip of
//     Sinai (Ras Muhammad ≈ 34.25°E, 27.73°N) on the WEST side, then transit
//     the Gulf of Suez (centerline ~32.8-33.5°E, 28-30°N) up to Suez city
//     (32.55°E, 29.95°N). A direct line from Hurghada to Suez slices through
//     the Sinai mainland.
//   • Western Mediterranean: the safe deep-water corridor runs ~38°N from
//     south of Sardinia (39°N south coast) westward, staying NORTH of the
//     Algerian coast (which sits at 36.6-36.9°N from Algiers to Annaba) and
//     SOUTH of the Balearic Islands (39.3°N south coast). At lng ≈ 1-2°E a
//     line dropping below 37°N will brush Algiers.
//   • Strait of Sicily: pass through the Cape Bon-Sicily channel between
//     Cape Bon (11°E, 37°N) and Pantelleria (12°E, 36.8°N) — centerline
//     near (11.5°E, 37.4°N). Going through Pantelleria-Sicily gap is
//     ~11 km wide; avoid.
//   • English Channel: mid-channel runs ~50.3°N at lng=0, then the Strait
//     of Dover funnels through ~(1.5°E, 51°N) between Dover (1.3°E, 51.13°N)
//     and Calais (1.85°E, 50.95°N). Any waypoint at (0-1°E, 51.2°N) sits
//     INSIDE Kent/SE England. Approach Rotterdam from the Strait of Dover
//     up the southern North Sea staying north of the Belgian coast.
//   • Brittany: Brittany sticks west to ~-5°W at lat 48.5°N. Stay west of
//     -7°W until past lat 49°N before turning east into the Channel.
//   • Sri Lanka: south coast at 5.95-6°N from lng 80-82°E. Routes from
//     Mumbai/Hormuz to Malacca must pass at lat ≤ 5.5°N when crossing
//     longitudes 79-83°E.
//   • Taiwan: the island spans 120-122°E, 21.9-25.3°N. Routes to North China
//     must use either (a) the Taiwan Strait centerline ~119°E west of the
//     Pescadores (Penghu at 119.5°E, 23.5°N), or (b) the Pacific east of
//     Taiwan at lng ≥ 122.5°E. Routes to Korea/Japan should use the eastern
//     Pacific path. A diagonal cut at 120-122°E, 22-24°N goes THROUGH Taiwan.
//   • Cape of Good Hope routes pass east of Madagascar (Madagascar = 43-50°E,
//     12-26°S), then south of South Africa at ≥35°S before turning into the
//     Atlantic. They never cross the Mozambique Channel.
window.ARCS = [
  // ═══ Arabian Gulf outflow via Strait of Hormuz ═══
  // Ras Tanura → Singapore: Persian Gulf → Hormuz → Arabian Sea → south of Sri Lanka → Malacca → Singapore
  { id: 'ras-sing', from: 'ras', to: 'singapore', risk: 'critical',
    path: [[50.17,26.64],[52,26.5],[55,26.3],[56.3,26.5],[58,25],[60,23],[62,20],[65,15],[70,10],[76,6],[81,4.5],[88,4.2],[95,4],[100,3.2],[102.5,2],[103.83,1.26]] },
  // Kharg → Ningbo: down through Singapore, then UP via Taiwan Strait centerline (west of Pescadores) to East China Sea
  { id: 'kharg-ningbo', from: 'kharg', to: 'ningbo', risk: 'critical',
    path: [[50.32,29.23],[51,28.5],[53,27.5],[55,26.8],[56.3,26.5],[58,25],[60,22],[64,17],[72,10],[80,5.5],[88,4.5],[95,4],[100,3.5],[103.83,1.26],[107,4],[111,9],[114,14],[117,18],[118,21],[118.7,23.5],[119,25.3],[120.3,27.5],[121.4,29],[121.54,29.87]] },
  // Basrah → Rotterdam: Persian Gulf → Hormuz → Arabian Sea → Bab el-Mandeb → Red Sea → Gulf of Suez → Suez Canal → Mediterranean → Gibraltar → Atlantic → English Channel → Rotterdam
  { id: 'basrah-rotter', from: 'basrah', to: 'rotter', risk: 'critical',
    path: [
      [47.82,30.5],[49,29.5],[51,28],[54,26.8],[56.3,26.5],[58,24.5],[59,21],[59,15],[57,12],[52,12],[48,12.3],[45,12.4],[43.3,12.6],
      [42.2,14],[41.3,16],[40.3,18],[39.3,20],[38.2,22],[37,24],[35.5,26],
      [34.5,27.2],[33.7,27.9],[33.0,28.7],[32.7,29.5],[32.55,29.95],[32.45,30.5],[32.3,31.2],[31.5,32],
      [25,33.5],[18,35],[15,36],[12.5,36.7],[11.5,37.4],[8,38],[3,38],[-1,37],[-3,36],[-5.5,36],[-7,36.3],[-9,36.7],
      [-10,42],[-11,46],[-7,48.5],[-5,49.5],[0,50.3],[2,51],[3,51.6],[4.14,51.95]
    ] },
  // Basrah → Ulsan: down Hormuz, around to Pacific east of Taiwan, up to Korea
  { id: 'kuwait-ulsan', from: 'basrah', to: 'ulsan', risk: 'critical',
    path: [[47.82,30.5],[49,29],[52,27.5],[55,26.8],[56.3,26.5],[58,24],[60,20],[65,13],[75,8],[88,5],[98,3.5],[103.83,1.26],[107,4],[111,9],[114,14],[117,18],[120,20],[122,22],[123,25],[125,28],[127,31],[128.5,33.5],[129.36,35.49]] },

  // ═══ Red Sea via Bab el-Mandeb → Suez → Europe ═══
  { id: 'jeddah-rotter', from: 'jeddah', to: 'rotter', risk: 'high',
    path: [
      [39.19,21.49],[37.8,23],[36.5,25],[35,26.8],
      [34.5,27.2],[33.7,27.9],[33.0,28.7],[32.7,29.5],[32.55,29.95],[32.45,30.5],[32.3,31.2],[31.5,32],
      [25,33.5],[18,35],[15,36],[12.5,36.7],[11.5,37.4],[8,38],[3,38],[-1,37],[-3,36],[-5.5,36],[-7,36.3],[-9,36.7],
      [-10,42],[-11,46],[-7,48.5],[-5,49.5],[0,50.3],[2,51],[3,51.6],[4.14,51.95]
    ] },

  // ═══ Cape of Good Hope alternative routes ═══
  // Cape → Houston: round Cape, NW across South Atlantic, then bend NORTH well
  // east of Brazil's eastern bulge (Cape São Roque at -35.5°W, -5.2°S — the
  // easternmost point of the Americas). NEVER cross west of -33°W between lat
  // -12°S and 0° or the line will cut through NE Brazil. Then NW across the
  // Atlantic, into the Caribbean north of Trinidad, south of Hispaniola/Cuba,
  // through the Yucatán Channel, into the Gulf of Mexico to Houston.
  { id: 'ras-houston-cape', from: 'ras', to: 'houston', risk: 'safe',
    path: [[50.17,26.64],[52,26.5],[55,26.3],[56.3,26.5],[58,24.5],[60,21],[62,15],[62,8],[58,0],[55,-10],[53,-20],[51,-28],[42,-35],[30,-37],[22,-36],[18.47,-34.35],[10,-34],[0,-32],[-10,-27],[-20,-20],[-28,-12],[-30,-5],[-32,0],[-40,5],[-50,10],[-60,13],[-68,15],[-80,20],[-88,23],[-93,27],[-95.28,29.72]] },
  // Cape route to Rotterdam: round Cape, due west into deep South Atlantic, north
  // through mid-Atlantic well west of the West-African bulge (Dakar at -17.5°W,
  // Conakry at -13.7°W, Cape Verde Islands at -24°W), then back ENE past Iberia
  // (Lisbon -9.1°W) into the English Channel via mid-channel, through Strait of
  // Dover, into Rotterdam. NEVER bend east of -19°W between lat 5°N and 25°N or
  // the path will cut through Senegal/Mauritania.
  { id: 'ras-rotter-cape', from: 'ras', to: 'rotter', risk: 'safe',
    path: [
      [50.17,26.64],[52,26.5],[55,26.3],[56.3,26.5],[58,24],[60,19],[62,12],[60,0],[58,-12],[55,-22],[52,-30],[42,-35],[30,-37],[22,-36],[18.47,-34.35],
      [10,-33],[0,-25],[-10,-10],[-15,5],[-20,12],[-22,18],[-20,25],[-15,32],[-12,38],[-11,42],[-12,46],[-7,48.5],[-5,49.5],[0,50.3],[2,51],[3,51.6],[4.14,51.95]
    ] },

  // ═══ Baseline regional flow ═══
  { id: 'ras-mumbai', from: 'ras', to: 'mumbai', risk: 'flow',
    path: [[50.17,26.64],[52,26.5],[55,26.3],[56.3,26.5],[58,24.5],[60,23],[63,21],[67,19.5],[71,19],[72.87,19.07]] },
  { id: 'jeddah-sing', from: 'jeddah', to: 'singapore', risk: 'flow',
    path: [[39.19,21.49],[40.5,18.5],[41.5,16],[42.2,14],[43.3,12.6],[45,12.5],[48,12.5],[52,12.5],[58,10],[65,8],[75,6],[81,4.8],[88,4.3],[95,4],[100,3.2],[103.83,1.26]] }
];

// Runtime state — hydrated from backend. NO hardcoded values here.
window.ATTACKS       = [];  // from /api/events (ACLED)
window.BRENT_SPARK   = [];  // from /api/brent
window.IRAN_EVENTS   = [];  // from /api/iran-events (curated)
window.FEED          = [];  // from /api/iran-events (news)
window.THESIS_EVENTS = [];  // from /api/thesis-events (726 ACLED historical)
window.MASTER        = null; // from /api/master
