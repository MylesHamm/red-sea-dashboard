/* ═══════════════════════════════════════════════════════════════════════════
   Main Application Logic — Tab navigation, data fetching, auto-refresh
   ═══════════════════════════════════════════════════════════════════════════ */

// ─── HTML Escape Helper (XSS prevention) ────────────────────────────────────

function _esc(str) {
    if (str == null) return '';
    const div = document.createElement('div');
    div.textContent = String(str);
    return div.innerHTML;
}

let masterData = null;
let eventsData = null;
let thesisEventsData = null;  // The 726 ACLED-verified maritime events analyzed in the thesis
let hypothesisData = null;
let iranEventsData = null;
let iranImpactData = null;
let currentTab = 'overview';
let _iranDataLoading = false;
let _iranDataLoaded = false;
let _acledLoaded = false;
let _acledLoading = false;
const _renderedTabs = new Set(['overview']);  // track which tabs have been rendered

// ─── Tab Navigation ─────────────────────────────────────────────────────────

document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        const tab = btn.dataset.tab;
        switchTab(tab);
    });
});

function switchTab(tab) {
    currentTab = tab;

    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

    document.querySelector(`.tab-btn[data-tab="${tab}"]`)?.classList.add('active');
    document.getElementById(`tab-${tab}`)?.classList.add('active');

    // Initialize map on first geospatial tab visit — uses thesis dataset, not full ACLED
    if (tab === 'geospatial') {
        initMap();
        resizeMap();
        const mapSource = thesisEventsData || eventsData;
        if (mapSource && allMapEvents.length === 0) {
            loadMapEvents(mapSource, !!thesisEventsData);
        }
    }

    // Initialize Iran/Israel map + resize charts on current events tab visit
    // Lazy-loads Iran data on first visit; re-renders charts on subsequent visits
    if (tab === 'currentevents') {
        if (!_iranDataLoaded && !_iranDataLoading) {
            _loadIranData();
        } else if (_iranDataLoaded) {
            setTimeout(() => {
                try {
                    if (iranEventsData) _loadIranIsraelMapFromData();
                    resizeIranIsraelMap();
                } catch (e) { console.warn('Iran map init:', e); }
                if (iranImpactData) {
                    const brentPrices = iranImpactData.brent_prices || (masterData ? masterData.timeseries.filter(d => d.brent_price).map(d => ({ date: d.date, price: d.brent_price })) : []);
                    const curated = iranEventsData ? iranEventsData.curated || [] : [];
                    const isWarZoom = document.getElementById('zoomWar')?.classList.contains('active') ?? true;
                    createIranPriceTimelineChart(brentPrices, curated, isWarZoom);
                    createIranForecastChart(brentPrices);
                    createIranImpactChart(iranImpactData.impact_by_type || {});
                    if (iranEventsData) createIranEventTypeChart(iranEventsData.data || []);
                }
            }, 150);
        }
    }

    // Render econometric tab on first visit + re-render charts for correct sizing
    if (tab === 'econometric' && masterData && hypothesisData) {
        if (!_renderedTabs.has('econometric')) {
            _renderedTabs.add('econometric');
            renderEconometric();
        }
        setTimeout(() => {
            createModelComparisonChart(hypothesisData);
            createPriceWindowChart(masterData.price_windows);
            createScatterChart(masterData.timeseries);
        }, 100);
    }

    // Render controls tab on first visit + re-render heatmap for correct sizing
    if (tab === 'controls' && masterData) {
        if (!_renderedTabs.has('controls')) {
            _renderedTabs.add('controls');
            renderControls();
        } else if (masterData.correlation) {
            setTimeout(() => createCorrelationChart(masterData.correlation), 100);
        }
    }

    // Render comparative analysis tab on first visit
    if (tab === 'comparative') {
        if (!_renderedTabs.has('comparative')) {
            _renderedTabs.add('comparative');
            _loadComparativeData();
        }
    }

    // Render data table on first visit; lazy-load ACLED events for table
    if (tab === 'data' && masterData) {
        if (!_renderedTabs.has('data')) {
            _renderedTabs.add('data');
            renderDataTable();
        }
        if (!_acledLoaded && !_acledLoading) {
            _acledLoading = true;
            fetchJSON('/api/events').then(events => {
                eventsData = events.data || [];
                if (!thesisEventsData.length) thesisEventsData = eventsData;
                _acledLoaded = true;
                renderDataTable();
            }).catch(() => {}).finally(() => { _acledLoading = false; });
        }
    }

    // Initialize maritime embeds and charts on first tab visit
    if (tab === 'maritime') {
        if (!_renderedTabs.has('maritime')) {
            _renderedTabs.add('maritime');
            renderMaritime();
        }
        initMaritimeEmbeds();
        setTimeout(() => {
            createSuezTransitChart();
            createBabElMandebChart();
            createHormuzChart();
        }, 150);
    }

    // Conclusion tab — static content, no special initialization needed
    // (show/hide handled by the generic tab-switching logic above)
}

// ─── Data Fetching ──────────────────────────────────────────────────────────

async function fetchJSON(url) {
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    return resp.json();
}

let _comparativeData = null;
async function _loadComparativeData() {
    if (_comparativeData) return;
    try {
        _comparativeData = await fetchJSON('/api/comparative');
        renderComparative(_comparativeData);
    } catch (e) {
        console.warn('Comparative data load failed:', e);
    }
}

function renderComparative(data) {
    if (!data) return;
    const s = data.summary || {};

    // KPIs
    const houthiRange = s.houthi_price_range;
    const iranRange = s.iran_price_range;
    if (houthiRange) animateValue('kpi-houthi-range', `$${houthiRange[0]} – $${houthiRange[1]}`);
    if (iranRange) animateValue('kpi-iran-range', `$${iranRange[0]} – $${iranRange[1]}`);
    if (s.iran_price_change_pct != null) animateValue('kpi-iran-impact', `${s.iran_price_change_pct > 0 ? '+' : ''}${s.iran_price_change_pct}%`);

    // Hormuz status
    const hd = data.hormuz_disruption || {};
    if (hd.status) {
        animateValue('kpi-hormuz-status', hd.status);
        const statusEl = document.getElementById('kpi-hormuz-status');
        if (statusEl) {
            const colors = { BLOCKADE: '#ff5252', RESTRICTED: '#F09060', DISRUPTED: '#ffab00', OPEN: '#00e676', UNKNOWN: '#8892a0' };
            statusEl.style.color = colors[hd.status] || '#8892a0';
        }
    }
    if (hd.pct_decline != null) {
        const declineEl = document.getElementById('kpi-hormuz-decline');
        if (declineEl) declineEl.textContent = `${hd.pct_decline}% decline from baseline`;
    }

    // Apply accent colors
    document.querySelectorAll('#tab-comparative .kpi-card[data-accent]').forEach(card => {
        card.style.borderTopColor = card.dataset.accent;
    });

    // Charts
    createComparativePriceChart(data);
    createIranIntensityChart(data.iran_intensity || []);
    createChokepointCompareChart(data.bab_transits || [], data.hormuz_transits || []);
    createVolatilityCompareChart(data);
}

async function _loadIranData() {
    if (_iranDataLoaded || _iranDataLoading) return;
    _iranDataLoading = true;
    updateStatus('loading', 'Loading Iran events & impact data...');
    try {
        const [iranEvents, iranImpact] = await Promise.all([
            fetchJSON('/api/iran-events').catch(() => ({ count: 0, data: [], curated: [] })),
            fetchJSON('/api/iran-impact').catch(() => ({ kpis: {}, impact_by_type: {}, event_table: [] })),
        ]);
        iranEventsData = iranEvents;
        iranImpactData = iranImpact;
        _iranDataLoaded = true;

        renderCurrentEvents();

        // If still on the tab, init map + charts
        if (currentTab === 'currentevents') {
            setTimeout(() => {
                try {
                    _loadIranIsraelMapFromData();
                    resizeIranIsraelMap();
                } catch (e) { console.warn('Iran map init:', e); }
                const brentPrices = iranImpactData.brent_prices || (masterData ? masterData.timeseries.filter(d => d.brent_price).map(d => ({ date: d.date, price: d.brent_price })) : []);
                const curated = iranEventsData ? iranEventsData.curated || [] : [];
                const isWarZoom = document.getElementById('zoomWar')?.classList.contains('active') ?? true;
                createIranPriceTimelineChart(brentPrices, curated, isWarZoom);
                createIranForecastChart(brentPrices);
                createIranImpactChart(iranImpactData.impact_by_type || {});
                if (iranEventsData) createIranEventTypeChart(iranEventsData.data || []);
            }, 150);
        }

        updateStatus('live', `Live — Iran data loaded`);
    } catch (err) {
        console.warn('Iran data load failed:', err);
        updateStatus('live', 'Live — Iran data unavailable');
    } finally {
        _iranDataLoading = false;
    }
}

async function loadAllData() {
    updateStatus('loading', 'Loading core data...');

    try {
        // Phase 1: Load fast, essential data first (CSV-backed, no external API)
        const [master, thesisEvents, hypothesis] = await Promise.all([
            fetchJSON('/api/master'),
            fetchJSON('/api/thesis-events').catch(() => null),
            fetchJSON('/api/hypothesis'),
        ]);

        masterData = master;
        thesisEventsData = (thesisEvents && thesisEvents.data) ? thesisEvents.data : [];
        hypothesisData = hypothesis;

        // Render only the visible tab immediately — defer others to tab switch
        renderOverview();

        // If map tab is active, load thesis events now
        if (currentTab === 'geospatial' && thesisEventsData.length) {
            loadMapEvents(thesisEventsData, true);
        }

        updateStatus('live', `Live — ${masterData.kpis.total_trading_days || masterData.timeseries.length} trading days loaded`);
        updateLastUpdated();

        // Phase 2: Heavy API calls load on-demand when tabs are visited
        // ACLED events only needed for Data Explorer tab — defer
        // Iran data deferred to currentevents tab visit (_loadIranData)
        if (currentTab === 'currentevents') {
            _loadIranData();
        }

    } catch (err) {
        console.error('Data load failed:', err);
        updateStatus('offline', 'Offline mode — using cached data');
    }
}

// ─── Render Functions ───────────────────────────────────────────────────────

function renderOverview() {
    if (!masterData) return;
    const { timeseries, kpis } = masterData;

    // KPI cards
    animateValue('kpi-brent', `$${kpis.latest_brent_price}`);
    animateValue('kpi-events', thesisEventsData && thesisEventsData.length ? thesisEventsData.length : 726);
    animateValue('kpi-dxy', kpis.latest_dxy || '--');
    animateValue('kpi-ovx', kpis.latest_ovx || '--');

    // War day counter
    const warStart = new Date('2026-02-28');
    const today = new Date();
    const warDay = Math.floor((today - warStart) / (1000 * 60 * 60 * 24));
    if (warDay > 0) animateValue('kpi-war-day', `Day ${warDay}`);

    // Hormuz status (async, non-blocking)
    fetchJSON('/api/hormuz-status').then(hd => {
        if (hd && hd.status) {
            animateValue('kpi-overview-hormuz', hd.status);
            const el = document.getElementById('kpi-overview-hormuz');
            if (el) {
                const colors = { BLOCKADE: '#ff5252', RESTRICTED: '#F09060', DISRUPTED: '#ffab00', OPEN: '#00e676', UNKNOWN: '#8892a0' };
                el.style.color = colors[hd.status] || '#8892a0';
            }
            if (hd.pct_decline != null) {
                const sub = document.getElementById('kpi-overview-hormuz-sub');
                if (sub) sub.textContent = `${hd.pct_decline}% decline from baseline`;
            }
        }
    }).catch(() => {});

    // Brent change indicator
    const changeEl = document.getElementById('kpi-brent-change');
    if (changeEl && kpis.brent_price_change !== undefined) {
        const change = kpis.brent_price_change;
        const arrow = change >= 0 ? '▲' : '▼';
        changeEl.textContent = `${arrow} $${Math.abs(change).toFixed(2)}`;
        changeEl.className = `kpi-change ${change >= 0 ? 'positive' : 'negative'}`;
    }

    // Apply accent colors to KPI cards
    document.querySelectorAll('.kpi-card[data-accent]').forEach(card => {
        card.style.borderTopColor = card.dataset.accent;
    });

    // Build event-date lookup for crossfiltering
    if (eventsData) {
        const eventDates = {};
        eventsData.forEach(e => {
            const type = e.event_type || 'Unknown';
            const date = (e.event_date || '').substring(0, 10);
            if (!eventDates[type]) eventDates[type] = [];
            eventDates[type].push(date);
        });
        // Convert arrays to Sets for O(1) lookup, attach to timeseries
        Object.keys(eventDates).forEach(k => eventDates[k] = eventDates[k]);
        timeseries._eventDates = eventDates;
    }

    // Charts
    createPriceAttackChart(timeseries);
    createVolatilityChart(timeseries);
    const chartEvents = thesisEventsData && thesisEventsData.length ? thesisEventsData : eventsData;
    if (chartEvents) createEventTypesChart(chartEvents);
}

// Re-render overview charts when crossfilter changes
function renderOverviewFiltered() {
    if (!masterData) return;
    const { timeseries } = masterData;

    // Rebuild event date lookup
    if (eventsData) {
        const eventDates = {};
        eventsData.forEach(e => {
            const type = e.event_type || 'Unknown';
            const date = (e.event_date || '').substring(0, 10);
            if (!eventDates[type]) eventDates[type] = [];
            eventDates[type].push(date);
        });
        timeseries._eventDates = eventDates;
    }

    createPriceAttackChart(timeseries);
    createVolatilityChart(timeseries);
    createScatterChart(timeseries);
    const chartEvents = thesisEventsData && thesisEventsData.length ? thesisEventsData : eventsData;
    if (chartEvents) createEventTypesChart(chartEvents);
}

function renderEconometric() {
    if (!hypothesisData || !masterData) return;

    // Hypothesis cards
    const container = document.getElementById('hypothesisCards');
    if (container) {
        container.innerHTML = '';
        const metricTooltips = {
            'Coefficient': 'The estimated effect size — how much the dependent variable changes per one-unit increase in the independent variable, holding all controls constant.',
            'P-Value': 'Statistical significance level. Values below 0.05 indicate the result is unlikely due to chance. Lower values = stronger evidence against the null hypothesis.',
            'R²': 'Coefficient of determination — the proportion of variance in the dependent variable explained by this model. Ranges from 0 (no fit) to 1 (perfect fit).'
        };
        ['h1', 'h2', 'h3'].forEach(key => {
            const h = hypothesisData[key];
            container.innerHTML += `
                <div class="hypothesis-card">
                    <div class="hypothesis-header">
                        <h4>${h.name}</h4>
                        <p>${h.description}</p>
                    </div>
                    <div class="hypothesis-body">
                        <div class="hypothesis-metrics">
                            <div class="metric">
                                <div class="metric-label">Coefficient <div class="chart-info"><span class="chart-info-icon">i</span><span class="chart-info-tooltip">${metricTooltips['Coefficient']}</span></div></div>
                                <div class="metric-value">${h.coefficient.toFixed(4)}</div>
                            </div>
                            <div class="metric">
                                <div class="metric-label">P-Value <div class="chart-info"><span class="chart-info-icon">i</span><span class="chart-info-tooltip">${metricTooltips['P-Value']}</span></div></div>
                                <div class="metric-value">${h.p_value.toFixed(4)}</div>
                            </div>
                            <div class="metric">
                                <div class="metric-label">R² <div class="chart-info"><span class="chart-info-icon">i</span><span class="chart-info-tooltip">${metricTooltips['R²']}</span></div></div>
                                <div class="metric-value">${h.r_squared.toFixed(4)}</div>
                            </div>
                        </div>
                        <div class="hypothesis-verdict ${h.supported ? 'verdict-supported' : 'verdict-not-supported'}">
                            ${h.conclusion}
                        </div>
                    </div>
                </div>
            `;
        });
    }

    // GARCH summary
    const garchEl = document.querySelector('#garchSummary .garch-details');
    if (garchEl && hypothesisData.garch_summary) {
        const g = hypothesisData.garch_summary;
        garchEl.innerHTML = `
            <div class="garch-item"><div class="garch-item-label">Model</div><div class="garch-item-value">${g.model}</div></div>
            <div class="garch-item"><div class="garch-item-label">Distribution</div><div class="garch-item-value">${g.distribution}</div></div>
            <div class="garch-item"><div class="garch-item-label">Observations</div><div class="garch-item-value">${g.observations}</div></div>
            <div class="garch-item"><div class="garch-item-label">Log-Likelihood</div><div class="garch-item-value">${g.log_likelihood}</div></div>
            <div class="garch-item"><div class="garch-item-label">AIC</div><div class="garch-item-value">${g.aic}</div></div>
            <div class="garch-item"><div class="garch-item-label">BIC</div><div class="garch-item-value">${g.bic}</div></div>
        `;
    }

    // Charts
    createModelComparisonChart(hypothesisData);
    createPriceWindowChart(masterData.price_windows);
    createScatterChart(masterData.timeseries);
}

function renderControls() {
    if (!masterData) return;
    const { timeseries, correlation } = masterData;

    createDxyOvxChart(timeseries);
    createGeopoliticalChart(timeseries);
    createSprChart(timeseries);
    if (hypothesisData) createControlCoefficientChart(hypothesisData);

    // Delay correlation chart to ensure canvas is visible
    setTimeout(() => createCorrelationChart(correlation), 200);
}

// ─── Current Events (US-Iran) ────────────────────────────────────────────

function renderCurrentEvents() {
    if (!iranEventsData || !iranImpactData) return;

    const kpis = iranImpactData.kpis || {};
    animateValue('kpi-iran-total', kpis.total_events || 0);
    animateValue('kpi-iran-price-move', kpis.avg_price_move_3d ? `$${kpis.avg_price_move_3d}` : '--');
    animateValue('kpi-iran-vol-spike', kpis.peak_volatility_spike ? `$${kpis.peak_volatility_spike}` : '--');
    animateValue('kpi-iran-month', kpis.events_this_month || 0);

    // Apply accent colors
    document.querySelectorAll('#tab-currentevents .kpi-card[data-accent]').forEach(card => {
        card.style.borderTopColor = card.dataset.accent;
    });

    // Get brent prices — prefer from iran-impact endpoint (includes yfinance supplement),
    // fall back to masterData timeseries
    let brentPrices = iranImpactData.brent_prices || [];
    if (!brentPrices.length && masterData) {
        brentPrices = masterData.timeseries.filter(d => d.brent_price).map(d => ({ date: d.date, price: d.brent_price }));
    }

    // Store for zoom toggle reuse
    window._iranBrentPrices = brentPrices;
    window._iranCurated = iranEventsData.curated || [];

    // Charts — default to zoomed war view
    createIranPriceTimelineChart(brentPrices, iranEventsData.curated || [], true);
    createIranForecastChart(brentPrices);
    createIranEventTypeChart(iranEventsData.data || []);
    createIranImpactChart(iranImpactData.impact_by_type || {});

    // Geospatial attack map — only if tab is currently visible
    if (currentTab === 'currentevents') {
        setTimeout(() => _loadIranIsraelMapFromData(), 200);
    }

    // Event impact table — grouped by day with before/after prices
    const tbody = document.getElementById('iranTableBody');
    if (tbody && iranImpactData.event_table) {
        const rows = iranImpactData.event_table.sort((a, b) => b.date.localeCompare(a.date));
        tbody.innerHTML = rows.map(day => {
            const changeCls = day.change_pct > 0 ? 'change-positive' : day.change_pct < 0 ? 'change-negative' : '';
            const changeText = day.change_pct != null ? `${day.change_pct > 0 ? '+' : ''}${day.change_pct}%` : '--';
            const events = day.events || [{ title: day.title, type: day.type, severity: day.severity }];
            const displayDate = day.display_date || day.date;
            const eventsList = events.map(e =>
                `<div class="day-event"><span class="event-type-badge type-${_esc(e.type)}">${_esc(e.type)}</span> ${_esc(e.title)}</div>`
            ).join('');
            return `
                <tr>
                    <td>${displayDate}</td>
                    <td class="day-events-cell">${eventsList}</td>
                    <td><span class="severity-badge severity-${day.severity}">${day.severity}</span></td>
                    <td>${day.brent_before != null ? '$' + day.brent_before.toFixed(2) : '--'}</td>
                    <td>${day.brent_after != null ? '$' + day.brent_after.toFixed(2) : '--'}</td>
                    <td class="${changeCls}">${changeText}</td>
                </tr>
            `;
        }).join('');
    }

    // Live Intelligence Feed
    renderNewsFeed(iranEventsData.news || []);
}

// ─── News Sentiment & Priority ──────────────────────────────────────────────

const _NEG_WORDS = /\b(strike|attack|killed|destroyed|threat|sanctions|missile|casualties|escalation|war|bomb|shot|dead|seized|blockade|crackdown)\b/i;
const _POS_WORDS = /\b(ceasefire|peace|agreement|diplomatic|talks|de-escalation|cooperation|deal|negotiate|resolution)\b/i;
const _PRIORITY_WORDS = /\b(Houthi|tanker|Hormuz|IRGC|strike|blockade|nuclear|uranium|carrier|Navy)\b/i;

function _newsSentiment(title) {
    if (!title) return 'neutral';
    const neg = (title.match(_NEG_WORDS) || []).length;
    const pos = (title.match(_POS_WORDS) || []).length;
    if (neg > pos) return 'negative';
    if (pos > neg) return 'positive';
    return 'neutral';
}

function _isPriorityNews(title) {
    return _PRIORITY_WORDS.test(title || '');
}

// ─── Live Intelligence Feed ──────────────────────────────────────────────────

function renderNewsFeed(news) {
    const container = document.getElementById('newsFeedContainer');
    if (!container || !news.length) {
        if (container) container.innerHTML = '<p class="news-feed-empty">No recent headlines available.</p>';
        return;
    }

    // Group articles by date
    const byDate = {};
    news.forEach(n => {
        if (!byDate[n.date]) byDate[n.date] = [];
        byDate[n.date].push(n);
    });
    const sortedDates = Object.keys(byDate).sort((a, b) => b.localeCompare(a));

    container.innerHTML = sortedDates.map(date => {
        const articles = byDate[date];
        const dateLabel = new Date(date + 'T12:00:00').toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
        return `
            <div class="news-date-group">
                <div class="news-date-header">
                    <span class="news-date-label">${dateLabel}</span>
                    <span class="news-date-count">${articles.length} article${articles.length !== 1 ? 's' : ''}</span>
                </div>
                <div class="news-articles">
                    ${articles.map(a => {
                        const sent = _newsSentiment(a.title);
                        const priority = _isPriorityNews(a.title);
                        const safeUrl = _esc(a.url || '#');
                        return `
                        <a href="${safeUrl}" target="_blank" rel="noopener" class="news-article">
                            <span class="sentiment-dot sentiment-${sent}"></span>
                            <span class="news-article-type type-${_esc(a.type)}">${_esc(a.type)}</span>
                            ${priority ? '<span class="priority-flag">PRIORITY</span>' : ''}
                            <span class="news-article-title">${_esc(a.title)}</span>
                            <span class="news-article-source">${_esc(a.source || '')}</span>
                        </a>`;
                    }).join('')}
                </div>
            </div>
        `;
    }).join('');
}

// ─── Timeline Zoom Toggle ───────────────────────────────────────────────────

let _iranZoomToWar = true;

function setTimelineZoom(zoomToWar) {
    _iranZoomToWar = zoomToWar;
    document.getElementById('zoomWar').classList.toggle('active', zoomToWar);
    document.getElementById('zoomFull').classList.toggle('active', !zoomToWar);
    if (window._iranBrentPrices) {
        createIranPriceTimelineChart(window._iranBrentPrices, window._iranCurated || [], zoomToWar);
    }
}

// Bind zoom toggle buttons via event listeners (scripts load at end of body, DOM is ready)
document.getElementById('zoomWar')?.addEventListener('click', () => setTimelineZoom(true));
document.getElementById('zoomFull')?.addEventListener('click', () => setTimelineZoom(false));

// ─── Iran Crossfilter Re-render ─────────────────────────────────────────────

// Helper: check if a date string falls within the Iran time slider range
function _inIranTimeRange(dateStr) {
    if (typeof _iranTimeRange === 'undefined' || !_iranTimeRange || !dateStr) return true;
    const ts = new Date(dateStr).getTime();
    return ts >= _iranTimeRange[0] && ts <= _iranTimeRange[1];
}

function renderCurrentEventsFiltered() {
    if (!iranEventsData || !iranImpactData) return;

    const brentPrices = window._iranBrentPrices || [];
    const curated = window._iranCurated || [];

    // Re-render timeline with current zoom + filter state
    createIranPriceTimelineChart(brentPrices, curated, _iranZoomToWar);

    // Re-render doughnut — filter by time range if slider is active
    let doughnutEvents = iranEventsData.data || [];
    if (typeof _iranTimeRange !== 'undefined' && _iranTimeRange) {
        doughnutEvents = doughnutEvents.filter(e => _inIranTimeRange(e.event_date || e.date));
    }
    createIranEventTypeChart(doughnutEvents);

    // Re-render impact bar chart (dims non-matching types)
    createIranImpactChart(iranImpactData.impact_by_type || {});

    // Re-filter event table (grouped by day)
    const tbody = document.getElementById('iranTableBody');
    if (tbody && iranImpactData.event_table) {
        let rows = iranImpactData.event_table.sort((a, b) => b.date.localeCompare(a.date));

        // Apply time slider filter
        rows = rows.filter(day => _inIranTimeRange(day.date));

        // Apply type crossfilter
        if (typeof iranFilter !== 'undefined' && iranFilter) {
            rows = rows.filter(day => {
                const events = day.events || [{ type: day.type }];
                return events.some(e => e.type === iranFilter.value);
            });
        }

        // Show filtered count
        const tableCount = document.getElementById('iranTableCount');
        if (tableCount) tableCount.textContent = `${rows.length} events`;

        tbody.innerHTML = rows.map(day => {
            const changeCls = day.change_pct > 0 ? 'change-positive' : day.change_pct < 0 ? 'change-negative' : '';
            const changeText = day.change_pct != null ? `${day.change_pct > 0 ? '+' : ''}${day.change_pct}%` : '--';
            const events = day.events || [{ title: day.title, type: day.type, severity: day.severity }];
            const displayDate = day.display_date || day.date;
            const eventsList = events.map(e =>
                `<div class="day-event"><span class="event-type-badge type-${_esc(e.type)}">${_esc(e.type)}</span> ${_esc(e.title)}</div>`
            ).join('');
            return `
                <tr>
                    <td>${displayDate}</td>
                    <td class="day-events-cell">${eventsList}</td>
                    <td><span class="severity-badge severity-${day.severity}">${day.severity}</span></td>
                    <td>${day.brent_before != null ? '$' + day.brent_before.toFixed(2) : '--'}</td>
                    <td>${day.brent_after != null ? '$' + day.brent_after.toFixed(2) : '--'}</td>
                    <td class="${changeCls}">${changeText}</td>
                </tr>
            `;
        }).join('');
    }
}

// Re-render curated timeline with current filters (type + time range)
function _rerenderCuratedTimeline() {
    if (!iranEventsData) return;
    const curated = iranEventsData.curated || [];
    let filtered = curated;

    // Apply time range filter
    filtered = filtered.filter(e => _inIranTimeRange(e.date));

    // Apply type crossfilter
    if (typeof iranFilter !== 'undefined' && iranFilter) {
        filtered = filtered.filter(e => e.type === iranFilter.value);
    }

    renderCuratedTimeline(filtered);
}

// ─── Data Table ─────────────────────────────────────────────────────────────

let tableData = [];
let tablePage = 1;
const tablePageSize = 25;
let sortCol = 'event_date';
let sortAsc = false;

function renderDataTable() {
    if (!eventsData) return;
    tableData = [...eventsData];

    // Populate event type filter
    const types = [...new Set(eventsData.map(e => e.event_type).filter(Boolean))];
    const select = document.getElementById('eventTypeFilter');
    if (select && select.options.length <= 1) {
        types.sort().forEach(t => {
            const opt = document.createElement('option');
            opt.value = t;
            opt.textContent = t;
            select.appendChild(opt);
        });
    }

    applyTableFilters();
}

function applyTableFilters() {
    const search = (document.getElementById('searchInput')?.value || '').toLowerCase();
    const type = document.getElementById('eventTypeFilter')?.value || '';
    const startDate = document.getElementById('dataStartDate')?.value || '';
    const endDate = document.getElementById('dataEndDate')?.value || '';

    tableData = eventsData.filter(e => {
        if (search && !(e.notes || '').toLowerCase().includes(search) &&
            !(e.location || '').toLowerCase().includes(search) &&
            !(e.actor1 || '').toLowerCase().includes(search)) return false;
        if (type && e.event_type !== type) return false;
        if (startDate && (e.event_date || '') < startDate) return false;
        if (endDate && (e.event_date || '') > endDate) return false;
        return true;
    });

    // Sort
    tableData.sort((a, b) => {
        let va = a[sortCol] || '';
        let vb = b[sortCol] || '';
        if (sortCol === 'fatalities') { va = parseInt(va) || 0; vb = parseInt(vb) || 0; }
        if (va < vb) return sortAsc ? -1 : 1;
        if (va > vb) return sortAsc ? 1 : -1;
        return 0;
    });

    tablePage = 1;
    renderTablePage();
}

function renderTablePage() {
    const tbody = document.getElementById('tableBody');
    if (!tbody) return;

    const start = (tablePage - 1) * tablePageSize;
    const end = start + tablePageSize;
    const page = tableData.slice(start, end);

    tbody.innerHTML = page.map(e => `
        <tr class="data-row-clickable" data-lat="${_esc(e.latitude || '')}" data-lng="${_esc(e.longitude || '')}">
            <td>${_esc((e.event_date || '').substring(0, 10))}</td>
            <td>${_esc(e.event_type || '')}</td>
            <td>${_esc(e.sub_event_type || '')}</td>
            <td>${_esc(e.actor1 || '')}</td>
            <td>${_esc(e.location || '')}</td>
            <td>${_esc(e.fatalities || 0)}</td>
        </tr>
    `).join('');

    // Sort indicators
    document.querySelectorAll('#dataTable th[data-sort] .sort-arrow').forEach(el => el.textContent = '↕');
    const activeTh = document.querySelector(`#dataTable th[data-sort="${sortCol}"] .sort-arrow`);
    if (activeTh) activeTh.textContent = sortAsc ? '▲' : '▼';

    // Pagination
    const totalPages = Math.ceil(tableData.length / tablePageSize);
    const pagDiv = document.getElementById('pagination');
    if (pagDiv) {
        pagDiv.innerHTML = '';
        const maxButtons = 10;
        let startPage = Math.max(1, tablePage - Math.floor(maxButtons / 2));
        let endPage = Math.min(totalPages, startPage + maxButtons - 1);
        if (endPage - startPage < maxButtons - 1) startPage = Math.max(1, endPage - maxButtons + 1);

        if (startPage > 1) {
            pagDiv.innerHTML += `<button onclick="goToPage(1)">1</button>`;
            if (startPage > 2) pagDiv.innerHTML += `<span style="padding:6px">...</span>`;
        }
        for (let i = startPage; i <= endPage; i++) {
            pagDiv.innerHTML += `<button class="${i === tablePage ? 'active' : ''}" onclick="goToPage(${i})">${i}</button>`;
        }
        if (endPage < totalPages) {
            if (endPage < totalPages - 1) pagDiv.innerHTML += `<span style="padding:6px">...</span>`;
            pagDiv.innerHTML += `<button onclick="goToPage(${totalPages})">${totalPages}</button>`;
        }
    }

    const info = document.getElementById('tableInfo');
    if (info) info.textContent = `Showing ${start + 1}–${Math.min(end, tableData.length)} of ${tableData.length} events (${(eventsData || []).length} total)`;

    // Row click → fly to event on geospatial map
    tbody.querySelectorAll('.data-row-clickable').forEach(row => {
        row.addEventListener('click', () => {
            const lat = parseFloat(row.dataset.lat);
            const lng = parseFloat(row.dataset.lng);
            if (isNaN(lat) || isNaN(lng)) return;
            switchTab('geospatial');
            setTimeout(() => {
                if (typeof map !== 'undefined' && map) map.flyTo([lat, lng], 10, { duration: 1 });
            }, 300);
        });
    });
}

function goToPage(p) {
    tablePage = p;
    renderTablePage();
}

// Table sorting
document.querySelectorAll('#dataTable th[data-sort]').forEach(th => {
    th.addEventListener('click', () => {
        const col = th.dataset.sort;
        if (sortCol === col) sortAsc = !sortAsc;
        else { sortCol = col; sortAsc = true; }
        applyTableFilters();
    });
});

// Filter buttons
document.getElementById('applyFilters')?.addEventListener('click', () => { applyTableFilters(); _updateStatusBar(); });
document.getElementById('resetFilters')?.addEventListener('click', () => {
    document.getElementById('searchInput').value = '';
    document.getElementById('eventTypeFilter').value = '';
    document.getElementById('dataStartDate').value = '2023-10-01';
    document.getElementById('dataEndDate').value = '2026-12-31';
    applyTableFilters();
    _updateStatusBar();
});

// Update status bar on any filter interaction across the dashboard
['iranMapEventTypeFilter', 'filterTankers', 'timeSlider', 'iranTimeSlider', 'eventTypeFilter', 'searchInput', 'dataStartDate', 'dataEndDate'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.addEventListener('change', () => setTimeout(_updateStatusBar, 100));
    if (el) el.addEventListener('input', () => setTimeout(_updateStatusBar, 100));
});

// Export CSV
document.getElementById('exportCsv')?.addEventListener('click', () => {
    if (!tableData.length) return;
    const headers = ['Date', 'Event Type', 'Sub-Event', 'Actor', 'Location', 'Fatalities', 'Notes'];
    const csvEscape = (val) => {
        const s = String(val == null ? '' : val);
        if (s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('\r')) {
            return '"' + s.replace(/"/g, '""') + '"';
        }
        return s;
    };
    const rows = tableData.map(e => [
        csvEscape((e.event_date || '').substring(0, 10)),
        csvEscape(e.event_type || ''),
        csvEscape(e.sub_event_type || ''),
        csvEscape(e.actor1 || ''),
        csvEscape(e.location || ''),
        csvEscape(e.fatalities || 0),
        csvEscape(e.notes || ''),
    ]);
    const csv = [headers.join(','), ...rows.map(r => r.join(','))].join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'acled_events_export.csv';
    a.click();
    URL.revokeObjectURL(url);
});

// ─── Utility Functions ──────────────────────────────────────────────────────

function animateValue(id, value) {
    const el = document.getElementById(id);
    if (!el) return;
    const text = String(value);
    // Try to extract numeric part for counting animation
    const match = text.match(/^(\$?~?)([\d,]+\.?\d*)(.*)/);
    if (!match) { el.textContent = value; return; }
    const prefix = match[1];
    const target = parseFloat(match[2].replace(/,/g, ''));
    const suffix = match[3];
    const hasDecimal = match[2].includes('.');
    const duration = 900;
    const start = performance.now();
    const startVal = parseFloat((el.textContent || '0').replace(/[^0-9.]/g, '')) || 0;
    function tick(now) {
        const elapsed = now - start;
        const progress = Math.min(elapsed / duration, 1);
        const eased = 1 - Math.pow(1 - progress, 3);
        const current = startVal + (target - startVal) * eased;
        const formatted = hasDecimal ? current.toFixed(2) : Math.round(current).toLocaleString();
        el.textContent = prefix + formatted + suffix;
        if (progress < 1) requestAnimationFrame(tick);
    }
    requestAnimationFrame(tick);
}

function updateStatus(state, text) {
    const dot = document.querySelector('.status-dot');
    const label = document.querySelector('.status-text');
    if (dot) {
        dot.className = 'status-dot';
        if (state === 'live') dot.classList.add('live');
        else if (state === 'offline') dot.classList.add('offline');
    }
    if (label) label.textContent = text;
}

function updateLastUpdated() {
    const el = document.getElementById('lastUpdated');
    if (el) {
        const now = new Date();
        el.textContent = `Last updated: ${now.toLocaleTimeString()}`;
    }
}

// ─── Iran/Israel Map Data Loader ─────────────────────────────────────────────

function _loadIranIsraelMapFromData() {
    if (!iranEventsData) return;

    // Iran ACLED events
    const iranAcled = iranEventsData.data || [];
    // Curated events with coordinates
    const curated = iranEventsData.curated || [];
    // Israel events — filter from main events dataset by country
    const israelEvents = (eventsData || []).filter(e =>
        e.country === 'Israel' || e.country === 'Palestine'
    );

    initIranIsraelMap();
    loadIranIsraelMap(iranAcled, israelEvents, curated);
    renderCuratedTimeline(curated);
}

// ─── Curated Event Timeline (full detail panel) ─────────────────────────────

function renderCuratedTimeline(events) {
    const container = document.getElementById('curatedTimelineContainer');
    const countEl = document.getElementById('curatedEventCount');
    if (!container || !events || !events.length) return;

    // Sort newest first
    const sorted = [...events].sort((a, b) => b.date.localeCompare(a.date));
    if (countEl) countEl.textContent = `${sorted.length} events`;

    container.innerHTML = sorted.map(e => {
        const severity = '\u2605'.repeat(e.severity || 0);
        const fatStr = e.fatalities ? `<div class="curated-timeline-fatalities">${Number(e.fatalities).toLocaleString()} fatalities</div>` : '';
        return `
            <div class="curated-timeline-item" data-lat="${_esc(e.lat)}" data-lon="${_esc(e.lon || e.lng)}">
                <div class="curated-timeline-date">${_esc(e.date)}</div>
                <div class="curated-timeline-body">
                    <div class="curated-timeline-title">${_esc(e.title)}</div>
                    <div class="curated-timeline-meta">
                        <span class="curated-timeline-type type-${_esc(e.type)}">${_esc(e.type)}</span>
                        <span class="curated-timeline-severity">${severity}</span>
                        <span class="curated-timeline-location">${_esc(e.location || '')}</span>
                    </div>
                    <div class="curated-timeline-desc">${_esc(e.description)}</div>
                    ${fatStr}
                </div>
            </div>
        `;
    }).join('');

    // Click to fly to location on map and open a detail popup
    container.querySelectorAll('.curated-timeline-item').forEach((el, idx) => {
        el.addEventListener('click', () => {
            const lat = parseFloat(el.dataset.lat);
            const lon = parseFloat(el.dataset.lon);
            if (!isNaN(lat) && !isNaN(lon) && typeof iranIsraelMap !== 'undefined' && iranIsraelMap) {
                document.querySelector('.iran-map-layout')?.scrollIntoView({ behavior: 'smooth' });
                setTimeout(() => {
                    iranIsraelMap.flyTo([lat, lon], 8, { duration: 1.2 });

                    // Build and open popup with full event details
                    const ev = sorted[idx];
                    if (ev) {
                        const fatLine = ev.fatalities
                            ? `<div style="margin-top:6px;font-size:11px;color:#ff4444;">FATALITIES: ${Number(ev.fatalities).toLocaleString()}</div>`
                            : '';
                        const popupHtml = `
                            <div class="tactical-popup">
                                <div style="color:#00d4ff;font-family:monospace;font-weight:700;margin-bottom:4px;">${_esc(ev.title)}</div>
                                <div style="color:#8892a0;font-size:11px;margin-bottom:6px;">${_esc(ev.date)} &middot; ${_esc(ev.location || '')}</div>
                                <div style="color:#c0c8d4;font-size:12px;line-height:1.5;">${_esc(ev.description || '')}</div>
                                ${fatLine}
                                <div style="margin-top:8px;font-size:11px;">
                                    <span style="color:#ffcc00;">TYPE: ${_esc((ev.type || '').toUpperCase())}</span> &middot;
                                    <span style="color:#ff6b6b;">SEVERITY: ${ev.severity || 0}/5</span>
                                </div>
                            </div>
                        `;
                        // Delay popup slightly so flyTo animation starts first
                        setTimeout(() => {
                            L.popup({ maxWidth: 360, className: 'dark-tactical-popup' })
                                .setLatLng([lat, lon])
                                .setContent(popupHtml)
                                .openOn(iranIsraelMap);
                        }, 600);
                    }
                }, 300);
            }
        });
    });
}

// ─── Maritime Traffic ────────────────────────────────────────────────────────

let maritimeEmbedsLoaded = false;

function renderMaritime() {
    // Apply accent colors to maritime KPI cards
    document.querySelectorAll('#tab-maritime .kpi-card[data-accent]').forEach(card => {
        card.style.borderTopColor = card.dataset.accent;
    });
    // Chart creation deferred to switchTab('maritime') so canvas is visible
}

function getMaritimeEmbedUrl(region) {
    const configs = {
        bab: { centery: 12.8, centerx: 43.3, zoom: 8 },
        hormuz: { centery: 26.4, centerx: 56.3, zoom: 8 },
    };
    const c = configs[region];
    // maptype:3 = dark theme, vtypes:8 = tankers only
    // Tighter zoom (8) to show density in the narrowest chokepoint passages
    return `https://www.marinetraffic.com/en/ais/embed/zoom:${c.zoom}/centery:${c.centery}/centerx:${c.centerx}/maptype:3/shownames:true/mmsi:0/shipid:0/fleet:/fleet_id:/vtypes:8/showmenu:true/remember:true`;
}

function initMaritimeEmbeds() {
    if (maritimeEmbedsLoaded) return;
    maritimeEmbedsLoaded = true;

    const container = document.getElementById('maritimeEmbedContainer');
    if (!container) return;

    // Replace placeholders with iframes
    container.innerHTML = `
        <iframe id="embedBab" src="${getMaritimeEmbedUrl('bab')}"
                loading="lazy" allowfullscreen
                title="Bab el-Mandeb AIS Vessel Tracker"></iframe>
        <iframe id="embedHormuz" src="${getMaritimeEmbedUrl('hormuz')}"
                loading="lazy" allowfullscreen
                title="Strait of Hormuz AIS Vessel Tracker"></iframe>
    `;
}

function setMaritimeView(mode) {
    const container = document.getElementById('maritimeEmbedContainer');
    if (!container) return;

    // Update toggle buttons
    ['viewBoth', 'viewBab', 'viewHormuz'].forEach(id => {
        document.getElementById(id)?.classList.remove('active');
    });
    const btnId = mode === 'both' ? 'viewBoth' : mode === 'bab' ? 'viewBab' : 'viewHormuz';
    document.getElementById(btnId)?.classList.add('active');

    const babFrame = document.getElementById('embedBab');
    const hormuzFrame = document.getElementById('embedHormuz');

    if (mode === 'both') {
        container.classList.remove('single-view');
        if (babFrame) babFrame.style.display = '';
        if (hormuzFrame) hormuzFrame.style.display = '';
    } else if (mode === 'bab') {
        container.classList.add('single-view');
        if (babFrame) babFrame.style.display = '';
        if (hormuzFrame) hormuzFrame.style.display = 'none';
    } else {
        container.classList.add('single-view');
        if (babFrame) babFrame.style.display = 'none';
        if (hormuzFrame) hormuzFrame.style.display = '';
    }
}

// Maritime event listeners
document.getElementById('viewBoth')?.addEventListener('click', () => setMaritimeView('both'));
document.getElementById('viewBab')?.addEventListener('click', () => setMaritimeView('bab'));
document.getElementById('viewHormuz')?.addEventListener('click', () => setMaritimeView('hormuz'));

// ─── Auto-Refresh ───────────────────────────────────────────────────────────

const REFRESH_INTERVAL = 30 * 60 * 1000; // 30 minutes
setInterval(() => {
    console.log('Auto-refreshing data...');
    loadAllData();
}, REFRESH_INTERVAL);

// ─── Manual Refresh Button ──────────────────────────────────────────────────

document.getElementById('refreshDataBtn')?.addEventListener('click', async () => {
    const btn = document.getElementById('refreshDataBtn');
    btn.disabled = true;
    btn.textContent = 'Refreshing...';
    try {
        await fetch('/api/refresh', { method: 'POST' });
        // Poll until backend refresh completes (check every 2s, max 60s)
        const maxWait = 60;
        const interval = 2;
        for (let waited = 0; waited < maxWait; waited += interval) {
            await new Promise(r => setTimeout(r, interval * 1000));
            try {
                const status = await fetchJSON('/api/refresh-status');
                if (!status.in_progress) break;
            } catch { break; }
            btn.textContent = `Refreshing... ${waited + interval}s`;
        }
        await loadAllData();
    } catch (e) {
        console.error('Refresh failed:', e);
    } finally {
        btn.textContent = 'Refresh Data';
        btn.disabled = false;
    }
});

// ─── Initialize ─────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
    loadAllData();

    // Restore last active tab from session (validate it exists)
    const savedTab = localStorage.getItem('activeTab');
    if (savedTab && document.getElementById(`tab-${savedTab}`)) switchTab(savedTab);

    // Build status bar
    _initStatusBar();

    // ─── Fixed-position tooltip positioning ─────────────────────────────────
    // .kpi-info-tooltip / .chart-info-tooltip use position:fixed so they escape
    // overflow:hidden parents.  On mouseenter we measure the tooltip off-screen,
    // compute top/left from the icon's bounding rect, then add a .tooltip-visible
    // class so CSS transitions handle the fade-in.  On mouseleave we remove it.
    function _positionTooltip(icon, tip) {
        // Move tooltip to body so it's free from any overflow/transform context
        if (tip.parentNode !== document.body) {
            document.body.appendChild(tip);
        }

        // Measure off-screen
        tip.style.cssText = 'position:fixed;display:block;visibility:hidden;opacity:0;' +
            'left:-9999px;top:-9999px;width:280px;z-index:99999;pointer-events:none;';
        const tipH = tip.offsetHeight;
        const tipW = tip.offsetWidth;
        const r = icon.getBoundingClientRect();

        // Center horizontally on the icon, clamp to viewport
        let left = r.left + r.width / 2 - tipW / 2;
        left = Math.max(8, Math.min(left, window.innerWidth - tipW - 8));

        // Prefer above the icon; flip below if clipped
        let top = r.top - tipH - 10;
        let flipped = false;
        if (top < 8) {
            top = r.bottom + 10;
            flipped = true;
        }

        // Apply position and show
        tip.style.cssText = 'position:fixed;display:block;visibility:visible;opacity:1;' +
            'width:280px;z-index:99999;pointer-events:none;' +
            'background:#111b2a;color:#c0cad8;font-size:12px;font-style:normal;' +
            'font-weight:400;line-height:1.5;padding:12px 14px;border-radius:8px;' +
            'border:1px solid rgba(0,212,255,0.2);' +
            'box-shadow:0 8px 24px rgba(0,0,0,0.5),0 0 15px rgba(0,212,255,0.08);' +
            'transition:opacity 0.15s ease;' +
            'left:' + left + 'px;top:' + top + 'px;';
        tip.classList.toggle('tooltip-below', flipped);
    }

    function _hideTooltip(tip) {
        tip.style.visibility = 'hidden';
        tip.style.opacity = '0';
        tip.classList.remove('tooltip-below');
    }

    // Clean up orphaned tooltips on tab switch
    const _origSwitchTabCleanup = switchTab;
    switchTab = function(tab) {
        document.querySelectorAll('.kpi-info-tooltip, .chart-info-tooltip').forEach(tip => {
            if (tip.parentNode === document.body) _hideTooltip(tip);
        });
        return _origSwitchTabCleanup(tab);
    };

    function initFixedTooltips() {
        // Attach to both KPI and chart info icons
        document.querySelectorAll('.kpi-info, .chart-info').forEach(wrapper => {
            const icon = wrapper.querySelector('.kpi-info-icon') || wrapper.querySelector('.chart-info-icon');
            const tip = wrapper.querySelector('.kpi-info-tooltip') || wrapper.querySelector('.chart-info-tooltip');
            if (!icon || !tip) return;

            // Store reference so we can find the tooltip after it's moved to body
            icon._tooltip = tip;

            icon.addEventListener('mouseenter', () => _positionTooltip(icon, icon._tooltip));
            icon.addEventListener('mouseleave', () => _hideTooltip(icon._tooltip));
        });
    }
    initFixedTooltips();
});

// ═══════════════════════════════════════════════════════════════════════════
// PALANTIR FUNCTIONALITY ENHANCEMENTS
// ═══════════════════════════════════════════════════════════════════════════

// ─── 1. Session Persistence ─────────────────────────────────────────────────

const _origSwitchTab = switchTab;
switchTab = function(tab) {
    _origSwitchTab(tab);
    localStorage.setItem('activeTab', tab);

    // Fade-in animation for tab content
    const content = document.getElementById(`tab-${tab}`);
    if (content) {
        content.style.opacity = '0';
        content.style.transform = 'translateY(6px)';
        requestAnimationFrame(() => {
            content.style.transition = 'opacity 0.3s ease, transform 0.3s ease';
            content.style.opacity = '1';
            content.style.transform = 'translateY(0)';
        });
    }
};

// ─── 2. Keyboard Navigation ─────────────────────────────────────────────────

document.addEventListener('keydown', (e) => {
    // Don't intercept when typing in inputs or with modifier keys
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT' || e.target.tagName === 'TEXTAREA') return;
    if (e.altKey) return;

    const tabs = Array.from(document.querySelectorAll('.tab-btn')).map(b => b.dataset.tab);
    const idx = tabs.indexOf(currentTab);

    switch (e.key) {
        case 'ArrowRight':
            if (idx < tabs.length - 1) switchTab(tabs[idx + 1]);
            e.preventDefault();
            break;
        case 'ArrowLeft':
            if (idx > 0) switchTab(tabs[idx - 1]);
            e.preventDefault();
            break;
        case 'r':
            if (!e.ctrlKey && !e.metaKey) {
                document.getElementById('refreshBtn')?.click();
                e.preventDefault();
            }
            break;
        case 'Escape':
            // Clear active crossfilter
            if (typeof clearIranFilter === 'function') clearIranFilter();
            break;
        case '/':
            const search = document.querySelector('#dataSearch');
            if (search) {
                switchTab('data');
                setTimeout(() => search.focus(), 200);
                e.preventDefault();
            }
            break;
        case 'f':
            if (!e.ctrlKey && !e.metaKey) _toggleFullscreenChart(e);
            break;
    }
});

// ─── 3. Chart Fullscreen ────────────────────────────────────────────────────

function _toggleFullscreenChart(e) {
    // Find closest chart card that might be "focused" (hovered)
    const hovered = document.querySelector('.chart-card:hover, .iran-map-grid:hover');
    if (!hovered) return;

    if (hovered.classList.contains('chart-fullscreen')) {
        hovered.classList.remove('chart-fullscreen');
        document.body.classList.remove('has-fullscreen');
    } else {
        hovered.classList.add('chart-fullscreen');
        document.body.classList.add('has-fullscreen');
        // Resize charts inside
        Object.values(chartInstances || {}).forEach(c => c?.resize?.());
    }
    e.preventDefault();
}

// Close fullscreen on Escape (backup)
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        document.querySelectorAll('.chart-fullscreen').forEach(el => el.classList.remove('chart-fullscreen'));
        document.body.classList.remove('has-fullscreen');
    }
});

// Double-click to fullscreen
document.addEventListener('dblclick', (e) => {
    const card = e.target.closest('.chart-card');
    if (!card) return;
    card.classList.toggle('chart-fullscreen');
    document.body.classList.toggle('has-fullscreen', document.querySelector('.chart-fullscreen'));
    Object.values(chartInstances || {}).forEach(c => c?.resize?.());
});

// ─── 4. Status Bar ──────────────────────────────────────────────────────────

function _initStatusBar() {
    const bar = document.createElement('div');
    bar.id = 'statusBar';
    bar.className = 'status-bar';
    bar.innerHTML = `
        <div class="status-item">
            <span class="status-pulse"></span>
            <span id="statusConnection">CONNECTED</span>
        </div>
        <div class="status-item">
            <span id="statusEvents">—</span> EVENTS LOADED
        </div>
        <div class="status-item">
            <span id="statusFreshness">—</span>
        </div>
        <div class="status-item">
            FILTERS: <span id="statusFilters">0</span> ACTIVE
        </div>
        <div class="status-item status-keys">
            <kbd>←</kbd><kbd>→</kbd> TABS &nbsp; <kbd>F</kbd> FULLSCREEN &nbsp; <kbd>R</kbd> REFRESH &nbsp; <kbd>/</kbd> SEARCH &nbsp; <kbd>ESC</kbd> CLEAR
        </div>
    `;
    document.body.appendChild(bar);

    // Update freshness timer
    setInterval(_updateStatusBar, 10000);
}

function _updateStatusBar() {
    // Events count
    const evtEl = document.getElementById('statusEvents');
    if (evtEl && eventsData) evtEl.textContent = eventsData.length?.toLocaleString() || '—';

    // Data freshness
    const freshEl = document.getElementById('statusFreshness');
    if (freshEl) {
        const updated = document.querySelector('.data-status span:last-child');
        if (updated) freshEl.textContent = updated.textContent;
    }

    // Filter count — all active filters across the dashboard
    const filterEl = document.getElementById('statusFilters');
    if (filterEl) {
        let count = 0;
        // Overview crossfilter (donut chart click)
        if (typeof activeFilter !== 'undefined' && activeFilter) count++;
        // Iran crossfilter (donut chart click)
        if (typeof iranFilter !== 'undefined' && iranFilter) count++;
        // Iran map event type dropdown
        const iranTypeF = document.getElementById('iranMapEventTypeFilter');
        if (iranTypeF && iranTypeF.value && iranTypeF.value !== 'all') count++;
        // Iran time slider (if not at full range)
        const iranSlider = document.getElementById('iranTimeSlider');
        if (iranSlider && iranSlider.value && parseInt(iranSlider.value) < parseInt(iranSlider.max || 100)) count++;
        // Red Sea tanker-only checkbox
        const tankerF = document.getElementById('filterTankers');
        if (tankerF && tankerF.checked) count++;
        // Red Sea time slider (if not at full range)
        const redSeaSlider = document.getElementById('timeSlider');
        if (redSeaSlider && redSeaSlider.value && parseInt(redSeaSlider.value) < parseInt(redSeaSlider.max || 100)) count++;
        // Data Explorer search
        const searchF = document.getElementById('searchInput');
        if (searchF && searchF.value.trim()) count++;
        // Data Explorer event type
        const typeF = document.getElementById('eventTypeFilter');
        if (typeF && typeF.value) count++;
        // Data Explorer date range (non-default)
        const startF = document.getElementById('dataStartDate');
        if (startF && startF.value && startF.value !== '2023-10-01') count++;
        const endF = document.getElementById('dataEndDate');
        if (endF && endF.value && endF.value !== '2026-12-31') count++;
        filterEl.textContent = count;
    }
}

// ─── 5. Live Pulse Animation ────────────────────────────────────────────────

// Add pulse to live badges after DOM loads
setTimeout(() => {
    document.querySelectorAll('.chart-badge').forEach(badge => {
        if (badge.textContent.trim().toLowerCase().includes('live')) {
            const pulse = document.createElement('span');
            pulse.className = 'live-pulse-dot';
            badge.prepend(pulse);
        }
    });
}, 2000);

// ─── 6. Map Coordinate Readout ──────────────────────────────────────────────

function _addCoordinateReadout(mapInstance, containerId) {
    const container = document.getElementById(containerId);
    if (!container || !mapInstance) return;

    const readout = document.createElement('div');
    readout.className = 'coord-readout';
    readout.innerHTML = '<span class="coord-lat">—</span> <span class="coord-lon">—</span>';
    container.closest('.map-container, .iran-map-grid')?.appendChild(readout);

    mapInstance.on('mousemove', (e) => {
        readout.querySelector('.coord-lat').textContent = `${e.latlng.lat.toFixed(4)}°N`;
        readout.querySelector('.coord-lon').textContent = `${e.latlng.lng.toFixed(4)}°E`;
    });
}

// Hook into map initialization
const _origInitMap = typeof initMap !== 'undefined' ? initMap : null;
if (_origInitMap) {
    initMap = function() {
        _origInitMap();
        setTimeout(() => {
            if (typeof map !== 'undefined' && map) _addCoordinateReadout(map, 'map');
        }, 500);
    };
}

// Hook into Iran map
const _origLoadIran = typeof loadIranIsraelMap !== 'undefined' ? loadIranIsraelMap : null;
if (_origLoadIran) {
    loadIranIsraelMap = function(...args) {
        const result = _origLoadIran(...args);
        setTimeout(() => {
            if (typeof iranIsraelMap !== 'undefined' && iranIsraelMap) _addCoordinateReadout(iranIsraelMap, 'iranIsraelMap');
        }, 500);
        return result;
    };
}
