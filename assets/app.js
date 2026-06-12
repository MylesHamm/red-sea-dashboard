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
      // Notify chart hydrators that a tab was activated. Charts whose
      // canvas was display:none on first paint render to a 0×0 surface
      // and stay invisible after the tab unhides — they need an explicit
      // re-render or .resize() once their parent has real dimensions.
      // (Symptom: §09b GDELT chart blank on the US-Iran tab until reload.)
      window.dispatchEvent(new CustomEvent('tab-changed', { detail: { name } }));
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
  // Skip rendering if BRENT_SPARK is missing/empty/single-point — earlier
  // versions checked truthiness only and an empty `[]` produced a fill
  // path like " L198,50 L2,50 Z" (no leading M), throwing an SVG console
  // error. hydrate.js takes over once /api/brent lands; this initial pass
  // is just for the fallback.
  if (sparkLine && Array.isArray(window.BRENT_SPARK) && window.BRENT_SPARK.length >= 2) {
    const data = window.BRENT_SPARK;
    const w = 200, h = 50, pad = 2;
    const min = Math.min(...data), max = Math.max(...data);
    const range = max - min || 1;
    const x = i => pad + (i/(data.length-1)) * (w - pad*2);
    const y = v => h - pad - ((v-min)/range) * (h - pad*2);
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
    vesselsAsof: document.getElementById('gpVesselsAsof'),
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
    gpEls.vessels.textContent = cp.vesselsInZone != null ? cp.vesselsInZone.toLocaleString() : '—';
    if (gpEls.vesselsAsof) {
      // Surface the data month so users understand "Monthly Transits" is
      // the most recent COMPLETE month from PortWatch — not a live count.
      // Without this label the static-looking number reads as broken.
      const months = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
      let asof = '';
      if (Array.isArray(cp.transitHistory) && cp.transitHistory.length) {
        const sorted = cp.transitHistory.slice().sort((a, b) => a.month.localeCompare(b.month));
        const latest = sorted[sorted.length - 1].month;
        const [y, mm] = latest.split('-').map(Number);
        if (y && mm) asof = `${months[mm-1]} ${y} · IMF PORTWATCH`;
      }
      gpEls.vesselsAsof.textContent = asof;
    }
    gpEls.incidents.textContent = cp.incidents30d != null ? cp.incidents30d : '—';

    gpEls.routes.innerHTML = (cp.routes || []).map(r => `
      <li><span class="r-dot r-${r.risk}"></span>${r.from} → ${r.to} <b>${r.mbd} mbd</b></li>
    `).join('');

    renderVesselList(id, cp);
  }

  // ── Sidebar "kill-zone incidents" list ──
  // Live AIS feeds are unreliable on free tiers (AISStream rate-limits a single
  // concurrent socket per key, MarineTraffic charges for the API). Instead the
  // sidebar reads the same ACLED-driven /api/chokepoint-incidents endpoint that
  // powers the analytical layers, so the list is an honest answer to "who is
  // operating against shipping in this zone right now."
  const INCIDENT_CHOKEPOINTS = new Set(['hormuz', 'bab', 'suez']);
  function escapeHtml(s) {
    return String(s == null ? '' : s).replace(/[&<>"]/g, c => (
      { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]
    ));
  }
  function classifySidebarEvent(ev) {
    const sub = (ev.sub_event_type || '').toLowerCase();
    const typ = (ev.event_type || '').toLowerCase();
    if (sub.includes('air/drone') || sub.includes('drone'))                                 return { tag: 'DRONE',   cls: 'v-type-drone' };
    if (sub.includes('shelling') || sub.includes('artillery') || sub.includes('missile'))  return { tag: 'MISSILE', cls: 'v-type-missile' };
    if (sub.includes('attack') && (typ.includes('battle') || typ.includes('violence')))    return { tag: 'NAVAL',   cls: 'v-type-naval' };
    if (sub.includes('abduction') || sub.includes('looting') || sub.includes('seizure'))   return { tag: 'HIJACK',  cls: 'v-type-hijack' };
    // Note: 'Protests' / 'Riots' event_types are now filtered out of
    // /api/chokepoint-incidents server-side (kill-zone view should not
    // include peaceful demonstrations or civil unrest). Kept here as
    // dead branches in case the upstream filter ever loosens — the tag
    // would still render correctly.
    return { tag: 'EVENT', cls: 'v-type-other' };
  }
  function relativeDays(iso, _ignoredAnchor) {
    // Always anchored to wall-clock now. Earlier versions accepted an
    // anchorTs that defaulted to dataset newest — that was misleading for
    // ACLED whose free-tier 12-month embargo means "newest event" can be
    // a year stale. The caller used to pass the dump's newest_ts here so
    // a 13-month-old event would render as "today" — which is not honest.
    // The arg is kept (ignored) so old call sites still typecheck.
    if (!iso) return '';
    const t = Date.parse(iso);
    if (!isFinite(t)) return '';
    const days = Math.max(0, Math.round((Date.now() - t) / 86400000));
    if (days === 0) return 'today';
    if (days === 1) return '1d ago';
    if (days < 30)  return `${days}d ago`;
    return `${Math.round(days / 30)}mo ago`;
  }
  function renderVesselList(id, cp) {
    if (!gpEls.vesselList) return;
    const captionEl = document.getElementById('gpVesselListCaption');
    const hint = 'color:var(--text-mute);font-family:var(--mono);font-size:11px;padding:6px 2px';

    function setCaption(text) {
      if (captionEl) captionEl.textContent = text || '';
    }

    if (!INCIDENT_CHOKEPOINTS.has(id)) {
      setCaption('');
      gpEls.vesselList.innerHTML = `<div style="${hint}">Incident overlay covers Hormuz, Bab el-Mandeb, and Suez.</div>`;
      return;
    }
    const bucket = window.CP && window.CP.incidents && window.CP.incidents[id];
    if (!bucket) {
      setCaption('');
      gpEls.vesselList.innerHTML = `<div style="${hint}">Loading incidents…</div>`;
      return;
    }
    if (bucket.error) {
      setCaption('');
      gpEls.vesselList.innerHTML = `<div style="${hint}">Incident feed unavailable (${escapeHtml(bucket.error)}).</div>`;
      return;
    }

    const events  = Array.isArray(bucket.events) ? bucket.events : [];
    const days    = Number(bucket.window_days) || 90;
    const newest  = bucket.zone_newest_date || null;

    // Caption discloses the dataset window so a stale ACLED feed (12-mo
    // embargo on the free tier) is visible at a glance. Format the newest
    // date as "DD MON YYYY" to match the rest of the UI.
    function fmtCaption(dateStr) {
      if (!dateStr) return 'ACLED · no in-zone events on record';
      const t = Date.parse(dateStr);
      if (!isFinite(t)) return `ACLED · data through ${escapeHtml(dateStr)}`;
      const d = new Date(t);
      const months = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
      const dd = String(d.getUTCDate()).padStart(2,'0');
      const mm = months[d.getUTCMonth()];
      const yy = d.getUTCFullYear();
      const ageDays = Math.max(0, Math.round((Date.now() - t) / 86400000));
      const stale = ageDays > 60 ? ` · ${ageDays > 365 ? Math.round(ageDays/30) + 'mo stale' : ageDays + 'd stale'}` : '';
      return `ACLED · data through ${dd} ${mm} ${yy}${stale}`;
    }
    setCaption(fmtCaption(newest));

    if (!events.length) {
      // Honest empty state. The earlier UI showed events from the dump's
      // newest 90-day window even when that window ended 13 months ago,
      // labeled with "Xd ago" relative to the dump's newest. Now: if the
      // wall-clock window is empty we say so plainly.
      const reason = newest
        ? `No incidents in the last ${days}d — most recent in-zone event is ${escapeHtml(newest)}.`
        : `No ACLED incidents on record for this zone.`;
      gpEls.vesselList.innerHTML = `<div style="${hint}">${reason}</div>`;
      return;
    }

    // Show the 6 most recent — sidebar is narrow, full set is on the §02.2 maps
    // (and the upstream ACLED dashboard via the footer link).
    const top = events.slice(0, 6);
    gpEls.vesselList.innerHTML = top.map(ev => {
      const cls   = classifySidebarEvent(ev);
      const where = escapeHtml(ev.location || ev.country || '—');
      const actor = escapeHtml((ev.actor1 || '').split(/[(:]/)[0].trim() || '—');
      const fat   = ev.fatalities && Number(ev.fatalities) > 0 ? `${Number(ev.fatalities)}†` : '—';
      const when  = escapeHtml(relativeDays(ev.date));
      return `<div class="vessel-row">
        <span class="v-type ${cls.cls}">${cls.tag}</span>
        <span class="v-name" title="${escapeHtml(ev.notes || '')}">${where} · ${actor}</span>
        <span class="v-speed">${when} · ${fat}</span>
      </div>`;
    }).join('');
  }

  // Poll /api/chokepoint-incidents/{cp} on an interval. The backend already
  // caches the underlying ACLED frame, so 5 min on the client is plenty —
  // events don't move and the dataset itself only refreshes a few times a day.
  async function refreshSidebarIncidents() {
    if (!window.API) return;
    window.CP = window.CP || {};
    window.CP.incidents = window.CP.incidents || {};
    // All three chokepoints with INCIDENT_BOUNDING_BOXES coverage. Originally
    // 'suez' was omitted here even though INCIDENT_CHOKEPOINTS includes it,
    // so the Suez sidebar permanently displayed "Loading incidents…" — the
    // bucket was never populated.
    const targets = ['hormuz', 'bab', 'suez'];
    const cutoff30 = Date.now() - 30 * 86400000;
    await Promise.all(targets.map(async (cp) => {
      try {
        const r = await window.API.chokepointIncidents(cp, 90);
        const events = Array.isArray(r && r.data) ? r.data : [];
        // No more dataset-newest anchor: relativeDays() now uses wall-clock
        // unconditionally (see app.js relativeDays). zone_newest_date is
        // surfaced separately for the panel caption.
        window.CP.incidents[cp] = {
          events,
          window_days:       r && r.days,
          zone_newest_date:  r && r.zone_newest_date,
          wall_clock_cutoff: r && r.wall_clock_cutoff,
          error: null,
        };
        // Promote the merged-pool 30-day count up to window.CHOKEPOINTS so
        // the hero "INCIDENTS · 30D" KPI and chokepoint cards reflect the
        // same wall-clock-recent oil-impactful events the kill-zone panel
        // shows. Without this the KPI fell back to raw-ACLED-only counts
        // which under the 12-month embargo collapse to 0.
        if (window.CHOKEPOINTS && window.CHOKEPOINTS[cp]) {
          let n = 0;
          for (const ev of events) {
            const t = Date.parse(ev.date || ev.event_date || '');
            if (isFinite(t) && t >= cutoff30) n++;
          }
          window.CHOKEPOINTS[cp].incidents30d = n;
        }
      } catch (e) {
        window.CP.incidents[cp] = { events: [], zone_newest_date: null, error: (e && e.message) || 'fetch failed' };
      }
    }));
    // Refresh hero "INCIDENTS · 30D" KPI from the broader oil-impact pool.
    refreshHeroIncidentsKpi();
    // Re-render the §02 chokepoint card grid so each card's per-chokepoint
    // INCIDENTS · 30D box picks up the merged-pool count we just wrote.
    try {
      if (typeof window.__hydrateChokepointCards === 'function') {
        window.__hydrateChokepointCards();
      }
    } catch (e) { /* non-fatal */ }
    renderChokepoint(currentChokepoint);
  }

  // Hero "INCIDENTS · 30D" KPI — globally deduped count of recent oil-impactful
  // events from curated war timeline + Google News (the same pool that feeds
  // §09 timeline and §11 feed). Earlier this was just summed across chokepoint
  // pools, which under-counted because the chokepoint endpoint caps the news
  // contribution to news-promoted (~5 entries per refresh) and ignored the
  // broader ~45-article live news feed.
  //
  // Called after /api/chokepoint-incidents fetches AND after /api/iran-events
  // lands ('data-hydrated' event), so the count converges to the right value
  // regardless of which API resolves first.
  // Cached server-computed dashboard state (from /api/dashboard-state).
  // This is the authoritative source; client-side dedup is a fallback for
  // the brief window before the first fetch resolves. Refreshed every
  // 30s by the hero KPI poller below.
  window.__dashState = null;

  function _writeDeltaKpi(deltaEl, d) {
    if (!deltaEl) return;
    const sign = d > 0 ? '+' : d < 0 ? '−' : '±';
    deltaEl.textContent = `${sign}${Math.abs(d)} vs prior 30D`;
    deltaEl.classList.toggle('up-alert', d > 0);
    deltaEl.classList.toggle('down', d < 0);
  }
  function _writeAsofKpi(asofEl) {
    if (!asofEl) return;
    const d = new Date();
    const months = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
    asofEl.textContent = `${String(d.getDate()).padStart(2,'0')} ${months[d.getMonth()]} ${d.getFullYear()}`;
  }

  function refreshHeroIncidentsKpi() {
    try {
      const incidentEl = document.querySelector('[data-kpi="incidents30"]');
      if (!incidentEl) return;

      // Preferred path: server-composed state. Server runs the same dedup
      // logic in Python and returns a single canonical number.
      const ds = window.__dashState;
      if (ds && ds.kpis && ds.kpis.incidents_30d) {
        const k = ds.kpis.incidents_30d;
        incidentEl.textContent = String(k.count != null ? k.count : '—');
        _writeDeltaKpi(document.querySelector('[data-kpi="incidents30Delta"]'), k.delta || 0);
        _writeAsofKpi(document.querySelector('[data-kpi-foot-asof="incidents30"]'));
        return;
      }

      // Fallback: client-side dedup of curated + news. Used only for the
      // first-paint window before /api/dashboard-state resolves.
      const iranState = (window.CP && window.CP.state && window.CP.state.iran) || {};
      const curated = Array.isArray(iranState.curated) ? iranState.curated : [];
      const news    = Array.isArray(iranState.news)    ? iranState.news    : [];
      const now      = Date.now();
      const cutoff30 = now - 30 * 86400000;
      const cutoff60 = now - 60 * 86400000;

      const seen = new Set();
      let total = 0, prior = 0;
      const accept = (date, title) => {
        if (!date) return;
        const key = `${date}::${(title || '').slice(0, 50).toLowerCase().trim()}`;
        if (seen.has(key)) return;
        seen.add(key);
        const t = Date.parse(date);
        if (!isFinite(t)) return;
        if (t >= cutoff30)      total++;
        else if (t >= cutoff60) prior++;
      };
      for (const c of curated) accept(c.date, c.title || c.label);
      for (const n of news)    accept(n.date, n.title || n.label);

      incidentEl.textContent = String(total);
      _writeDeltaKpi(document.querySelector('[data-kpi="incidents30Delta"]'), total - prior);
      _writeAsofKpi(document.querySelector('[data-kpi-foot-asof="incidents30"]'));
    } catch (e) { /* non-fatal */ }
  }

  // Tick flash: when a live update changes a price, pulse the element
  // green (up) or red (down) so the update is visible instead of silent.
  // Re-adding the class mid-animation requires a reflow nudge — removing
  // the class alone doesn't restart a running CSS animation.
  function _tickFlash(el, dir) {
    if (!el || !dir) return;
    el.classList.remove('tick-up', 'tick-down');
    void el.offsetWidth;  // force reflow so the animation restarts
    el.classList.add(dir > 0 ? 'tick-up' : 'tick-down');
  }

  // Apply the FMP intraday Brent quote to the hero KPI when dashboard-state
  // reports it. Falls back silently to whatever hydrate.js wrote (EIA daily
  // settle) if FMP is unavailable. Adds an "INTRADAY · FMP" badge so users
  // know the price is minute-fresh, not yesterday's settle.
  let _lastAppliedBrent = null;
  function _applyHeroBrentFromState(ds) {
    const b = ds && ds.kpis && ds.kpis.brent;
    if (!b || b.price == null) return;
    const newPrice = Number(b.price);
    const tickDir = (_lastAppliedBrent != null && newPrice !== _lastAppliedBrent)
      ? (newPrice > _lastAppliedBrent ? 1 : -1) : 0;
    _lastAppliedBrent = newPrice;
    const heroPriceEl = document.getElementById('heroPrice');
    if (heroPriceEl) {
      heroPriceEl.textContent = newPrice.toFixed(2);
      _tickFlash(heroPriceEl, tickDir);
    }
    // Threat-strip Brent
    const threatBrent = document.getElementById('threatBrent');
    if (threatBrent) {
      threatBrent.textContent = '$' + newPrice.toFixed(2);
      _tickFlash(threatBrent, tickDir);
    }
    // Hero change line
    const heroChange = document.getElementById('heroChange');
    if (heroChange && b.change_24h != null) {
      const c = +b.change_24h;
      const p = b.change_24h_pct != null ? +b.change_24h_pct : null;
      const arrow = c > 0 ? '▲' : c < 0 ? '▼' : '•';
      // Format: "+$1.70 (+1.55%)" or "−$1.70 (−1.55%)" — the dollar sign
      // stays attached to the unsigned magnitude so the sign reads
      // cleanly. Earlier this rendered as "$-1.70" which is awkward.
      const fmt = (n) => (n < 0 ? '−' : (n > 0 ? '+' : '')) + '$' + Math.abs(n).toFixed(2);
      const fmtPct = (n) => (n < 0 ? '−' : (n > 0 ? '+' : '')) + Math.abs(n).toFixed(2) + '%';
      heroChange.innerHTML = `<span class="change-arrow">${arrow}</span> ${fmt(c)}` +
        (p != null ? ` (${fmtPct(p)})` : '') + ' · 24h';
      heroChange.classList.toggle('up',   c > 0);
      heroChange.classList.toggle('down', c < 0);
    }
    // Methodology footer — live values
    try {
      const fmt$ = v => v == null ? '—' : '$' + Number(v).toFixed(2);
      const fmt  = v => v == null ? '—' : Number(v).toFixed(1);
      const buildEl = document.querySelector('[data-build-id]');
      if (buildEl && !buildEl.textContent.trim()) {
        // Asset cache-buster doubles as a build id (mtime hash)
        const link = document.querySelector('link[rel=stylesheet][href*="?v="]');
        const m = link && /\?v=([A-Za-z0-9]+)/.exec(link.href);
        buildEl.textContent = m ? m[1].slice(0, 7) : '—';
      }
      const setFoot = (k, v) => {
        const el = document.querySelector(`[data-foot-${k}]`);
        if (el) el.textContent = v;
      };
      setFoot('brent', fmt$(b.price));
      setFoot('ovx',   fmt(ds.kpis && ds.kpis.ovx && ds.kpis.ovx.value));
      const now = new Date();
      const months = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
      setFoot('asof', `${String(now.getDate()).padStart(2,'0')} ${months[now.getMonth()]} ${now.getFullYear()} ${String(now.getHours()).padStart(2,'0')}:${String(now.getMinutes()).padStart(2,'0')}`);
    } catch (e) { /* non-fatal */ }

    // SINCE WAR ONSET % must use the SAME price as the hero ($108.17 from
    // FMP, not $110.69 from EIA). hydrateHero originally computed this
    // from the EIA daily-settle series, leaving the page showing two
    // numbers anchored to two different prices. Server-side dashboard-
    // state.kpis.brent.war_premium_pct is FMP-anchored when intraday is
    // live; use it directly so the hero price and the % below it agree.
    const heroFootEl = document.querySelector('.kpi-hero-foot');
    if (heroFootEl && b.war_premium_pct != null) {
      const sinceSpan = heroFootEl.querySelector('[data-war-prem]');
      const wp = Number(b.war_premium_pct);
      const fmtWp = (wp >= 0 ? '+' : '') + wp.toFixed(1) + '%';
      if (sinceSpan) {
        sinceSpan.textContent = fmtWp;
        sinceSpan.className = wp >= 0 ? 'up' : 'down';
        sinceSpan.setAttribute('data-war-prem', '');
      }
      const threatWarPremEl = document.getElementById('threatWarPrem');
      if (threatWarPremEl) threatWarPremEl.textContent = fmtWp;
    }

    // Source/freshness badge — small text under the change line
    const heroFoot = document.querySelector('.kpi-hero-foot');
    if (heroFoot && b.source) {
      let badge = heroFoot.querySelector('[data-brent-source-badge]');
      if (!badge) {
        badge = document.createElement('span');
        badge.setAttribute('data-brent-source-badge', '');
        badge.style.cssText = 'opacity:0.55;font-size:9px;letter-spacing:1.5px;display:inline-block;margin-left:8px;color:' + (b.is_intraday ? '#3dd49b' : '#7e8699');
        heroFoot.appendChild(badge);
      }
      // Honest source labeling: FMP = real-time, yfinance = ~15-min
      // delayed futures quote (used when FMP is rate-limited), EIA =
      // previous daily settle. Never label delayed data as real-time.
      const src = String(b.source || '').toLowerCase();
      badge.textContent = !b.is_intraday ? 'EIA DAILY'
        : src.includes('yfinance')       ? 'INTRADAY · YF · ~15MIN DELAY'
        :                                  'INTRADAY · FMP';
      badge.style.color = b.is_intraday ? '#3dd49b' : '#7e8699';
    }
  }

  // Market alert strip: appears above the KPI deck when the 24h Brent
  // move exceeds the configured threshold (config.BRENT_ALERT_THRESHOLD_PCT
  // via /api/constants). Dismissal is per-alert-signature in
  // sessionStorage — dismissing a +2.3% alert won't suppress a later
  // −4% alert, but the same alert won't nag on every state push.
  function _updateMarketAlert(ds) {
    const b = ds && ds.kpis && ds.kpis.brent;
    const deck = document.querySelector('.kpi-deck');
    if (!deck) return;
    let strip = document.getElementById('marketAlertStrip');
    const thresh = (window.CONSTANTS && window.CONSTANTS.brent_alert_threshold_pct != null)
      ? +window.CONSTANTS.brent_alert_threshold_pct : 2.0;
    const pct = b && b.change_24h_pct != null ? +b.change_24h_pct : null;
    const active = pct != null && Math.abs(pct) >= thresh;
    // Signature: direction + integer band, so ±0.1% drift doesn't retrigger
    const sig = active ? `${pct > 0 ? 'up' : 'down'}:${Math.floor(Math.abs(pct))}` : '';
    let dismissed = '';
    try { dismissed = sessionStorage.getItem('brentAlertDismissed') || ''; } catch (_) {}
    if (!active || sig === dismissed) {
      if (strip) strip.remove();
      return;
    }
    if (!strip) {
      strip = document.createElement('div');
      strip.id = 'marketAlertStrip';
      strip.className = 'market-alert';
      deck.parentElement.insertBefore(strip, deck);
    }
    const up = pct > 0;
    strip.classList.toggle('alert-up', up);
    strip.classList.toggle('alert-down', !up);
    const arrow = up ? '▲' : '▼';
    const fmtPct = (up ? '+' : '−') + Math.abs(pct).toFixed(2) + '%';
    strip.innerHTML =
      `<span class="ma-pulse"></span>` +
      `<span class="ma-text">MARKET ALERT · BRENT ${arrow} ${fmtPct} IN 24H` +
      `${b.price != null ? ` · $${Number(b.price).toFixed(2)}` : ''}` +
      ` · ${up ? 'RISK PREMIUM BUILDING' : 'RISK PREMIUM UNWINDING'}</span>` +
      `<button class="ma-dismiss" title="Dismiss this alert">×</button>`;
    strip.querySelector('.ma-dismiss').addEventListener('click', () => {
      try { sessionStorage.setItem('brentAlertDismissed', sig); } catch (_) {}
      strip.remove();
    });
  }

  // Apply a composed dashboard-state payload to the page. Single entry
  // point shared by the SSE stream (push) and the watchdog poller (pull)
  // so both paths produce identical UI updates.
  function applyDashboardState(ds) {
    if (!ds || !ds.kpis) return;
    window.__dashState = ds;
    refreshHeroIncidentsKpi();
    _applyHeroBrentFromState(ds);
    _updateMarketAlert(ds);
    // Notify chart renderers that depend on dashboard-state. §01
    // reads weekly_oil_events from here; without a re-render, the
    // chart paints once with an empty oilByWeek map and the
    // post-Oct bars stay at 0 even after the state lands.
    window.dispatchEvent(new CustomEvent('dashboard-state-ready', { detail: ds }));
  }

  async function refreshDashboardState() {
    if (!window.API || !window.API.dashboardState) return;
    try {
      const ds = await window.API.dashboardState();
      applyDashboardState(ds);
    } catch (e) { /* non-fatal — fallback path will run */ }
  }

  // ── Live push via Server-Sent Events ────────────────────────────────
  // /api/stream sends the full dashboard-state whenever the backend
  // warmer lands fresh data (plus a 'ping' heartbeat every ~25s).
  // EventSource auto-reconnects on transient drops; the 60s poller
  // below acts as a watchdog and only fires when the stream has been
  // silent for >90s (i.e. genuinely dead, not merely idle — pings
  // count as life).
  let __sseLastSeen = 0;
  (function startStateStream() {
    if (!window.EventSource) return;
    let es;
    try { es = new EventSource((window.__API_BASE || '') + '/api/stream'); } catch (_) { return; }
    es.onopen = () => { __sseLastSeen = Date.now(); };
    es.addEventListener('ping', () => { __sseLastSeen = Date.now(); });
    es.addEventListener('state', (ev) => {
      __sseLastSeen = Date.now();
      try { applyDashboardState(JSON.parse(ev.data)); } catch (_) {}
    });
    // No onerror handler needed: EventSource retries automatically, and
    // while it's down __sseLastSeen ages out and the poller takes over.
  })();

  refreshDashboardState();  // immediate first paint — don't wait for SSE
  setInterval(() => {
    if (Date.now() - __sseLastSeen < 90_000) return;  // SSE is alive
    if (document.hidden) return;                       // tab not visible
    refreshDashboardState();
  }, 60_000);

  // Instant catch-up when the user returns to the tab. Browsers throttle
  // hidden-tab timers (and may have dropped the SSE connection during a
  // laptop sleep), so the state can be minutes stale at the moment of
  // return — refresh immediately rather than waiting for the next tick.
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) return;
    if (window.API && window.API.invalidate) {
      window.API.invalidate('/api/dashboard-state');
      window.API.invalidate('/api/freshness');
    }
    refreshDashboardState();
  });
  // Recompute when iran data finishes loading (it lands later than the
  // initial chokepoint-incidents fetch on cold start). Also expose on
  // window so hydrate.js's /api/events callback can delegate here instead
  // of writing its own (stale) number — there should be exactly one
  // hero-KPI setter to avoid the flicker the user saw ("59 → 0").
  window.__refreshHeroIncidentsKpi = refreshHeroIncidentsKpi;
  window.addEventListener('data-hydrated', refreshHeroIncidentsKpi);
  window.addEventListener('events-ready',  refreshHeroIncidentsKpi);
  refreshSidebarIncidents();
  setInterval(refreshSidebarIncidents, 300_000);

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
  // Track maps 0%→100% onto 2023-10-07 (Hamas attack) → wall-clock today.
  // Earlier versions hardcoded END to a frozen date; that meant the scrubber
  // ran out of room as the calendar advanced and the "CURRENT" position
  // pointed at a stale date (e.g. Apr 22 instead of today).
  // ═════════════════════════════════════════════════════════════════════
  (function initTimeline() {
    const track  = document.querySelector('.ts-track');
    const tsFill = document.getElementById('tsFill');
    const tsThumb = document.getElementById('tsThumb');
    const tsDate  = document.getElementById('tsDate');
    const tsButtons = document.querySelectorAll('.ts-btn');
    if (!track || !tsFill || !tsThumb || !tsDate) return;

    const _MONTHS = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
    const _now = new Date();
    const _todayIso = `${_now.getFullYear()}-${String(_now.getMonth()+1).padStart(2,'0')}-${String(_now.getDate()).padStart(2,'0')}`;
    const _todayLabel = `${String(_now.getDate()).padStart(2,'0')} ${_MONTHS[_now.getMonth()]} ${_now.getFullYear()}`;

    const START = new Date('2023-10-07').getTime();
    const END   = _now.getTime();
    const SPAN  = END - START;

    // Preset positions (in pct) tied to real dates
    const epochMap = {
      pre:    { date: '2023-11-01', label: 'NOV 2023' },
      houthi: { date: '2023-12-01', label: 'DEC 2023' },
      war:    { date: '2026-02-28', label: 'FEB 2026' },
      now:    { date: _todayIso,    label: _todayLabel }
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

    // Populate the "CURRENT" button label with today's month/year so it
    // doesn't display a stale "Apr 2026" when wall-clock is past that.
    const nowLabelEl = document.querySelector('[data-ts-now-label]');
    if (nowLabelEl) {
      const monthsTitle = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
      nowLabelEl.textContent = `${monthsTitle[_now.getMonth()]} ${_now.getFullYear()}`;
    }
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
  // Default the TO date to wall-clock today so the explorer never silently
  // hides events newer than a hardcoded value left in the markup.
  if (dpTo && !dpTo.value) {
    const _d = new Date();
    dpTo.value = `${_d.getFullYear()}-${String(_d.getMonth()+1).padStart(2,'0')}-${String(_d.getDate()).padStart(2,'0')}`;
  }
  const dpApply  = document.getElementById('dpApply');
  const dpExport = document.getElementById('dpExport');
  const dpMeta   = document.getElementById('dpMeta');
  const MAX_ROWS = 500;

  function dpEvents() {
    const s = window.CP && window.CP.state;
    return (s && Array.isArray(s.events) && s.events.length) ? s.events : (window.THESIS_EVENTS || []);
  }

  // The main /api/events payload is served LITE (no `notes`) so the Incidents
  // tab loads fast. When the user first clicks into the search box or opens
  // the Data Explorer, lazy-fetch the FULL payload with notes so full-text
  // search works. One-shot — we upgrade the cached state in place and then
  // never re-fetch until the page reloads.
  let dpFullLoaded = false;
  let dpFullPromise = null;
  function dpEnsureFullEvents() {
    if (dpFullLoaded || dpFullPromise) return dpFullPromise || Promise.resolve();
    if (!window.API || !window.API.eventsFull) return Promise.resolve();
    dpFullPromise = window.API.eventsFull()
      .then(r => {
        const full = (r && r.data) || [];
        if (full.length && window.CP && window.CP.state) {
          window.CP.state.events = full;
          dpFullLoaded = true;
          // Re-render whatever the user is currently viewing so notes-aware
          // search sees the upgraded payload.
          try { dpRender(); } catch (_) {}
        }
      })
      .catch(() => {})
      .finally(() => { dpFullPromise = null; });
    return dpFullPromise;
  }
  // Trigger the full fetch on first interaction with the overlay. Focus on
  // search, typing in search, or opening the overlay via the nav button all
  // count as "user actually wants to dig in."
  if (dpSearch) {
    dpSearch.addEventListener('focus', dpEnsureFullEvents, { once: true });
  }
  if (openBtn) {
    openBtn.addEventListener('click', () => setTimeout(dpEnsureFullEvents, 300), { once: true });
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
    // thesis observation window. Toggles only affect tactical-map layer
    // visibility — they do not recompute the underlying KPIs, which are
    // model inputs and must stay constant.
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

  // The §02.2 chokepoint panels embed MarineTraffic via iframe (live AIS).
  // The sidebar "kill-zone incidents" list is fed by /api/chokepoint-incidents
  // and rendered above by refreshSidebarIncidents + renderVesselList — no
  // Leaflet/AIS-WebSocket plumbing required here.

  // ── GDELT retry button (in the §09b empty-state overlay) ────────────────
  // The chart sometimes lands on an empty client-side cached response when
  // the backend's first GDELT call happens during cold start (GDELT slow).
  // Backend now skips caching empty results, but the FRONTEND cache still
  // holds the empty response for 30 min. This button bypasses that cache
  // and re-fetches live so users don't have to wait or hard-refresh.
  document.addEventListener('click', async (ev) => {
    const btn = ev.target.closest('[data-gdelt-retry]');
    if (!btn) return;
    if (!window.API) return;
    const original = btn.textContent;
    btn.textContent = '⟳ FETCHING…';
    btn.style.pointerEvents = 'none';
    try {
      API.invalidate('/api/gdelt-tone');
      const g = await API.gdeltTone();
      if (typeof window.__renderGdelt === 'function') {
        window.__renderGdelt(g);
      } else {
        // Fallback: just reload the page
        location.reload();
      }
    } catch (e) {
      btn.textContent = original;
      btn.style.pointerEvents = '';
      console.warn('gdelt retry failed', e);
    }
  });

  // ── ACLED diagnostic surfacing (click the "WHY?" badge) ──────────────────
  // Hits /api/diag and shows the actual exception string from the most
  // recent ACLED fetch attempt. Common causes:
  //   • "401 Unauthorized" or "missing access_token" → ACLED creds invalid
  //     or expired. Fix: rotate ACLED_USERNAME / ACLED_PASSWORD env vars
  //     on Render.
  //   • "ReadTimeout / ConnectionError" → ACLED API slow or unreachable
  //     from Render's egress. Fix: try /api/refresh again later; if
  //     persistent, check ACLED status page or contact ACLED support.
  //   • "credentials_configured: false" → env vars not set on Render.
  //   • "0 events for every query" → ACLED accepted the request but the
  //     date range / actor filter is too narrow (unlikely with our
  //     queries, but possible if ACLED schema changes).
  // Click handler for the §03 'WHY?' badge AND the inline panel's close
  // button. Renders the diag inline below the chart subtitle (instead of
  // alert(), which Chrome blocks after auto-dismissed prompts) so the
  // info is persistent and screenshot-friendly.
  document.addEventListener('click', async (ev) => {
    // Close button on the inline panel
    const closeBtn = ev.target.closest('.xf-diag-close');
    if (closeBtn) {
      const panel = closeBtn.closest('.xf-diag-panel');
      if (panel) panel.remove();
      return;
    }

    const tag = ev.target.closest('[data-acled-diag]');
    if (!tag) return;
    ev.preventDefault();
    if (!window.API) return;

    // Find / create the inline panel right after the subtitle that holds the badge.
    const subtitleEl = tag.closest('.nb-subtitle, [data-event-mix-subtitle]') || tag.parentElement;
    const host = subtitleEl && subtitleEl.parentElement;
    if (!host) return;
    let panel = host.querySelector('.xf-diag-panel');
    if (!panel) {
      panel = document.createElement('div');
      panel.className = 'xf-diag-panel';
      // Insert right after the subtitle line, before the chart canvas.
      subtitleEl.insertAdjacentElement('afterend', panel);
    }
    panel.innerHTML = '<span class="xf-diag-close" title="Close">✕</span><span class="xf-diag-key">Loading /api/diag…</span>';

    try {
      const d = await API.diag();
      const a = (d && d.acled) || {};
      const fmtAge = (s) => {
        if (s == null) return '(no cache file)';
        if (s < 60) return `${s}s`;
        if (s < 3600) return `${Math.round(s/60)}m`;
        if (s < 86400) return `${Math.round(s/3600)}h`;
        return `${Math.round(s/86400)}d`;
      };
      const esc = (v) => String(v == null ? '' : v).replace(/[&<>"]/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
      const k  = (label, val, errClass) =>
        `<span class="xf-diag-key">${label.padEnd(22, ' ')}</span> <span class="${errClass ? 'xf-diag-err' : ''}">${esc(val)}</span>`;
      const errStr = a.last_fetch_error || '(no error — last fetch succeeded; data freshness is upstream-gated)';

      // Diagnose the most likely root cause from the symptom pattern so
      // the user sees the answer instead of having to interpret raw
      // diagnostic fields.
      let rootCause = '';
      const newestStr = (a.max || '').slice(0, 10);
      let newestAge = null;
      if (newestStr) {
        try {
          newestAge = Math.floor((Date.now() - new Date(newestStr + 'T00:00:00Z').getTime()) / 86400000);
        } catch (_) {}
      }
      if (a.last_fetch_source === 'api' && newestAge != null && newestAge >= 300 && newestAge <= 430) {
        rootCause = [
          'ℹ️  WORKING AS DESIGNED · ACLED HYBRID DATA MODEL',
          '',
          '   This dashboard intentionally uses two ACLED feeds:',
          '',
          '   • Row-level events (this §03 donut + Incidents tab):',
          '     pulled from ACLED\'s free-tier OAuth API. Their academic',
          '     tier embargoes row-level data for ~12 months — that\'s why',
          '     the newest event you see is ' + (newestAge || '~365') + 'd behind today.',
          '     Trade-off: rich event-type / actor / lat-lon detail, but',
          '     a one-year publication lag.',
          '',
          '   • Monthly aggregates (§01 chart bars + chokepoint cards +',
          '     freshness pill): pulled live from HDX, where ACLED',
          '     republishes country-level summaries WITHOUT the embargo.',
          '     Trade-off: only event counts (no per-event detail), but',
          '     refreshed weekly within ~7 days of wall-clock.',
          '',
          '   Together these give an honest picture: rich category mix for',
          '   the historical Houthi window + live counts for the active',
          '   US-Iran war. Premium ACLED tier (~$/mo) would let the donut',
          '   show real-time mix, but the dashboard is functional without it.',
          '',
        ].join('\n');
      } else if (!a.credentials_configured) {
        rootCause = '🛑  ROOT CAUSE: ACLED env vars not set on Render. Set\n   ACLED_USERNAME and ACLED_PASSWORD in the Render dashboard.\n\n';
      } else if (a.last_fetch_source === 'fallback' && (a.last_fetch_error || '').match(/timeout|ConnectionError/i)) {
        rootCause = '🛑  ROOT CAUSE: ACLED API unreachable from Render (timeout).\n   Click "⟳ REFRESH ACLED" to retry, or check ACLED status.\n\n';
      } else if ((a.last_fetch_error || '').match(/401|missing access_token|invalid_grant/i)) {
        rootCause = '🛑  ROOT CAUSE: ACLED credentials rejected. Rotate\n   ACLED_USERNAME / ACLED_PASSWORD on Render.\n\n';
      }

      const html = [
        '<span class="xf-diag-close" title="Close">✕</span>',
        '<span class="xf-diag-key">ACLED FETCH DIAGNOSTIC</span>',
        '─────────────────────────',
        rootCause ? '<span class="xf-diag-err">' + esc(rootCause) + '</span>' : '',
        k('credentials_configured', a.credentials_configured),
        k('last_fetch_source', a.last_fetch_source || '(none)'),
        k('last_fetch_utc', a.last_fetch_utc || '(never)'),
        k('cache_file_age', fmtAge(a.cache_file_age_s)),
        k('query_date_range', a.query_date_range || '(unknown)'),
        k('token_present', a.token_present),
        k('token_expires_utc', a.token_expires_utc || '(no token)'),
        k('token_expires_in', a.token_expires_in_s != null ? fmtAge(a.token_expires_in_s) : '(no token)'),
        k('events in memo', a.count || 0),
        k('oldest event', a.min || '(empty)'),
        k('newest event', a.max || '(empty)') + (newestAge != null ? '  (' + newestAge + 'd behind today)' : ''),
        '',
        '<span class="xf-diag-key">last_fetch_error:</span>',
        '  <span class="xf-diag-err">' + esc(errStr) + '</span>',
        '',
        '<span class="xf-diag-key">Server time:</span> ' + esc(d && d.server_time_utc),
      ].filter(Boolean).join('\n');
      panel.innerHTML = html;
    } catch (e) {
      panel.innerHTML = '<span class="xf-diag-close" title="Close">✕</span><span class="xf-diag-err">Failed to load /api/diag: ' + (e && e.message || e) + '</span>';
    }
  });

  // ── Force-refresh button (any [data-action="refresh-acled"]) ─────────────
  // Fires POST /api/refresh on the backend, which clears API-driven caches
  // and re-fetches ACLED + Iran + Brent + EIA + FRED + GDELT + HDX in
  // parallel. The backend returns immediately (the work happens in a
  // background task), so we poll briefly and reload the page once the
  // refresh-status endpoint reports completion. Without this affordance
  // a stale ACLED cache stays stuck until the next 5-min auto-refresh
  // — a poor UX when the user can SEE the data is months old.
  document.addEventListener('click', async (ev) => {
    const btn = ev.target.closest('[data-action="refresh-acled"]');
    if (!btn) return;
    if (!window.API) return;
    const original = btn.textContent;
    btn.disabled = true;
    btn.textContent = '⟳ REFRESHING…';
    try {
      await API.refresh();
      // Poll status; backend takes ~30-90s for full re-fetch.
      let done = false;
      const t0 = Date.now();
      while (!done && (Date.now() - t0) < 120_000) {
        await new Promise(r => setTimeout(r, 4000));
        try {
          const s = await API.refreshStatus();
          if (s && s.in_progress === false) { done = true; break; }
        } catch (_) {}
      }
      btn.textContent = '⟳ DONE · RELOADING';
      // Cache-bust the API caches and reload
      API.invalidate();
      setTimeout(() => location.reload(), 800);
    } catch (e) {
      btn.disabled = false;
      btn.textContent = original;
      alert('Refresh failed: ' + (e && e.message || e));
    }
  });
});
