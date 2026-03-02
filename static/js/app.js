/* ═══════════════════════════════════════════════════════════════════════════
   Main Application Logic — Tab navigation, data fetching, auto-refresh
   ═══════════════════════════════════════════════════════════════════════════ */

let masterData = null;
let eventsData = null;
let hypothesisData = null;
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

    // Re-render correlation heatmap when controls tab becomes visible
    // (canvas needs visible parent for correct sizing)
    if (tab === 'controls' && masterData && masterData.correlation) {
        setTimeout(() => createCorrelationChart(masterData.correlation), 100);
    }
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
        const [master, events, hypothesis] = await Promise.all([
            fetchJSON('/api/master'),
            fetchJSON('/api/events'),
            fetchJSON('/api/hypothesis'),
        ]);

        masterData = master;
        eventsData = events.data || [];
        hypothesisData = hypothesis;

        renderOverview();
        renderEconometric();
        renderControls();
        renderDataTable();

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
    animateValue('kpi-volatility', kpis.peak_volatility);
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

    // Charts
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
                                <div class="metric-label">Coefficient</div>
                                <div class="metric-value">${h.coefficient.toFixed(4)}</div>
                            </div>
                            <div class="metric">
                                <div class="metric-label">P-Value</div>
                                <div class="metric-value">${h.p_value.toFixed(4)}</div>
                            </div>
                            <div class="metric">
                                <div class="metric-label">R²</div>
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
    document.getElementById('dataEndDate').value = '2025-12-31';
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

// ─── Auto-Refresh ───────────────────────────────────────────────────────────

const REFRESH_INTERVAL = 30 * 60 * 1000; // 30 minutes
setInterval(() => {
    console.log('Auto-refreshing data...');
    loadAllData();
}, REFRESH_INTERVAL);

// ─── Initialize ─────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
    loadAllData();
});
