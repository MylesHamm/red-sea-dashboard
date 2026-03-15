/* ═══════════════════════════════════════════════════════════════════════════
   Main Application Logic — Tab navigation, data fetching, auto-refresh
   ═══════════════════════════════════════════════════════════════════════════ */

let masterData = null;
let eventsData = null;
let hypothesisData = null;
let iranEventsData = null;
let iranImpactData = null;
let currentTab = 'overview';

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

    // Initialize map on first geospatial tab visit
    if (tab === 'geospatial') {
        initMap();
        resizeMap();
        if (eventsData && allMapEvents.length === 0) {
            loadMapEvents(eventsData);
        }
    }

    // Initialize Iran/Israel map + resize charts on current events tab visit
    // (Chart.js needs a visible parent to compute correct canvas dimensions)
    if (tab === 'currentevents') {
        setTimeout(() => {
            try {
                if (iranEventsData) _loadIranIsraelMapFromData();
                resizeIranIsraelMap();
            } catch (e) { console.warn('Iran map init:', e); }
            // Re-render charts that may have been created while tab was hidden (0×0 canvas)
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

    // Re-render correlation heatmap when controls tab becomes visible
    // (canvas needs visible parent for correct sizing)
    if (tab === 'controls' && masterData && masterData.correlation) {
        setTimeout(() => createCorrelationChart(masterData.correlation), 100);
    }

    // Initialize maritime embeds and chart on first tab visit
    if (tab === 'maritime') {
        initMaritimeEmbeds();
        // Defer chart creation until canvas is visible (avoids 0px sizing)
        setTimeout(() => createSuezTransitChart(), 150);
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

async function loadAllData() {
    updateStatus('loading', 'Loading data...');

    try {
        // Fetch master data and events in parallel
        const [master, events, hypothesis, iranEvents, iranImpact] = await Promise.all([
            fetchJSON('/api/master'),
            fetchJSON('/api/events'),
            fetchJSON('/api/hypothesis'),
            fetchJSON('/api/iran-events').catch(() => ({ count: 0, data: [], curated: [] })),
            fetchJSON('/api/iran-impact').catch(() => ({ kpis: {}, impact_by_type: {}, event_table: [] })),
        ]);

        masterData = master;
        eventsData = events.data || [];
        hypothesisData = hypothesis;
        iranEventsData = iranEvents;
        iranImpactData = iranImpact;

        renderOverview();
        renderEconometric();
        renderControls();
        renderDataTable();
        renderCurrentEvents();
        renderMaritime();

        // If map tab is active, load events
        if (currentTab === 'geospatial' && eventsData.length) {
            loadMapEvents(eventsData);
        }

        updateStatus('live', `Live — ${eventsData.length} events loaded`);
        updateLastUpdated();

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
    animateValue('kpi-events', eventsData ? eventsData.length : '--');
    animateValue('kpi-volatility', kpis.peak_volatility != null ? `${kpis.peak_volatility.toFixed(2)}%` : '--');
    animateValue('kpi-attacks', kpis.max_weekly_attacks);
    animateValue('kpi-dxy', kpis.latest_dxy || '--');
    animateValue('kpi-ovx', kpis.latest_ovx || '--');

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
    if (eventsData) createEventTypesChart(eventsData);
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
    if (eventsData) createEventTypesChart(eventsData);
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
                `<div class="day-event"><span class="event-type-badge type-${e.type}">${e.type}</span> ${e.title}</div>`
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
                    ${articles.map(a => `
                        <a href="${a.url || '#'}" target="_blank" rel="noopener" class="news-article">
                            <span class="news-article-type type-${a.type}">${a.type}</span>
                            <span class="news-article-title">${a.title}</span>
                            <span class="news-article-source">${a.source || ''}</span>
                        </a>
                    `).join('')}
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

function renderCurrentEventsFiltered() {
    if (!iranEventsData || !iranImpactData) return;

    const brentPrices = window._iranBrentPrices || [];
    const curated = window._iranCurated || [];

    // Re-render timeline with current zoom + filter state
    createIranPriceTimelineChart(brentPrices, curated, _iranZoomToWar);

    // Re-render doughnut (dims non-selected segments)
    createIranEventTypeChart(iranEventsData.data || []);

    // Re-render impact bar chart (dims non-matching types)
    createIranImpactChart(iranImpactData.impact_by_type || {});

    // Re-filter event table (grouped by day)
    const tbody = document.getElementById('iranTableBody');
    if (tbody && iranImpactData.event_table) {
        let rows = iranImpactData.event_table.sort((a, b) => b.date.localeCompare(a.date));
        if (typeof iranFilter !== 'undefined' && iranFilter) {
            // Filter days that contain at least one event of the filtered type
            rows = rows.filter(day => {
                const events = day.events || [{ type: day.type }];
                return events.some(e => e.type === iranFilter.value);
            });
        }
        tbody.innerHTML = rows.map(day => {
            const changeCls = day.change_pct > 0 ? 'change-positive' : day.change_pct < 0 ? 'change-negative' : '';
            const changeText = day.change_pct != null ? `${day.change_pct > 0 ? '+' : ''}${day.change_pct}%` : '--';
            const events = day.events || [{ title: day.title, type: day.type, severity: day.severity }];
            const displayDate = day.display_date || day.date;
            const eventsList = events.map(e =>
                `<div class="day-event"><span class="event-type-badge type-${e.type}">${e.type}</span> ${e.title}</div>`
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
        <tr>
            <td>${(e.event_date || '').substring(0, 10)}</td>
            <td>${e.event_type || ''}</td>
            <td>${e.sub_event_type || ''}</td>
            <td>${e.actor1 || ''}</td>
            <td>${e.location || ''}</td>
            <td>${e.fatalities || 0}</td>
        </tr>
    `).join('');

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
    if (info) info.textContent = `Showing ${start + 1}–${Math.min(end, tableData.length)} of ${tableData.length} events`;
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
document.getElementById('applyFilters')?.addEventListener('click', applyTableFilters);
document.getElementById('resetFilters')?.addEventListener('click', () => {
    document.getElementById('searchInput').value = '';
    document.getElementById('eventTypeFilter').value = '';
    document.getElementById('dataStartDate').value = '2023-10-01';
    document.getElementById('dataEndDate').value = '2026-12-31';
    applyTableFilters();
});

// Export CSV
document.getElementById('exportCsv')?.addEventListener('click', () => {
    if (!tableData.length) return;
    const headers = ['Date', 'Event Type', 'Sub-Event', 'Actor', 'Location', 'Fatalities', 'Notes'];
    const rows = tableData.map(e => [
        (e.event_date || '').substring(0, 10),
        e.event_type || '',
        e.sub_event_type || '',
        e.actor1 || '',
        e.location || '',
        e.fatalities || 0,
        `"${(e.notes || '').replace(/"/g, '""')}"`,
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
    if (el) el.textContent = value;
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
        const fatStr = e.fatalities ? `<div class="curated-timeline-fatalities">${e.fatalities.toLocaleString()} fatalities</div>` : '';
        return `
            <div class="curated-timeline-item" data-lat="${e.lat}" data-lon="${e.lon || e.lng}">
                <div class="curated-timeline-date">${e.date}</div>
                <div class="curated-timeline-body">
                    <div class="curated-timeline-title">${e.title}</div>
                    <div class="curated-timeline-meta">
                        <span class="curated-timeline-type type-${e.type}">${e.type}</span>
                        <span class="curated-timeline-severity">${severity}</span>
                        <span class="curated-timeline-location">${e.location || ''}</span>
                    </div>
                    <div class="curated-timeline-desc">${e.description}</div>
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
                                <div style="color:#00d4ff;font-family:monospace;font-weight:700;margin-bottom:4px;">${ev.title}</div>
                                <div style="color:#8892a0;font-size:11px;margin-bottom:6px;">${ev.date} &middot; ${ev.location || ''}</div>
                                <div style="color:#c0c8d4;font-size:12px;line-height:1.5;">${ev.description || ''}</div>
                                ${fatLine}
                                <div style="margin-top:8px;font-size:11px;">
                                    <span style="color:#ffcc00;">TYPE: ${(ev.type || '').toUpperCase()}</span> &middot;
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
        bab: { centery: 13.5, centerx: 43.5, zoom: 7 },
        hormuz: { centery: 26.5, centerx: 56.0, zoom: 7 },
    };
    const c = configs[region];
    // maptype:3 = dark theme, vtypes:8 = tankers (AIS first-digit code: 8=Tanker)
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
        // Wait for background task, then reload frontend data
        setTimeout(async () => {
            await loadAllData();
            btn.textContent = 'Refresh Data';
            btn.disabled = false;
        }, 3000);
    } catch (e) {
        console.error('Refresh failed:', e);
        btn.textContent = 'Refresh Data';
        btn.disabled = false;
    }
});

// ─── Initialize ─────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
    loadAllData();

    // Restore last active tab from session
    const savedTab = localStorage.getItem('activeTab');
    if (savedTab) switchTab(savedTab);

    // Build status bar
    _initStatusBar();
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
    // Don't intercept when typing in inputs
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT' || e.target.tagName === 'TEXTAREA') return;

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

    // Filter count
    const filterEl = document.getElementById('statusFilters');
    if (filterEl) {
        let count = 0;
        if (typeof iranFilter !== 'undefined' && iranFilter) count++;
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
