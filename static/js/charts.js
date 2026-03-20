/* ═══════════════════════════════════════════════════════════════════════════
   Chart Definitions — All Chart.js configurations
   ═══════════════════════════════════════════════════════════════════════════ */

// HTML escape helper (shared with app.js via global scope)
function _escChart(str) {
    if (str == null) return '';
    const div = document.createElement('div');
    div.textContent = String(str);
    return div.innerHTML;
}

const COLORS = {
    brent: '#3D99D4',
    brentBg: 'rgba(61, 153, 212, 0.15)',
    attacks: '#ff5252',
    attacksBg: 'rgba(255, 82, 82, 0.30)',
    volatility: '#ffab00',
    volatilityBg: 'rgba(255, 171, 0, 0.12)',
    positive: '#00e676',
    negative: '#ff5252',
    dxy: '#9B7BEE',
    dxyBg: 'rgba(155, 123, 238, 0.12)',
    ovx: '#E07B4C',
    ovxBg: 'rgba(224, 123, 76, 0.12)',
    navy: '#0d1420',
    gold: '#D4B870',
    gray: '#8892a0',
    gridLine: 'rgba(0, 212, 255, 0.08)',
};

const CHART_DEFAULTS = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
        legend: {
            position: 'top',
            labels: {
                color: '#8892a0',
                font: { family: 'Inter', size: 11, weight: '500' },
                padding: 16,
                usePointStyle: true,
                pointStyleWidth: 8,
            }
        },
        tooltip: {
            backgroundColor: '#0d1420',
            borderColor: 'rgba(0, 212, 255, 0.3)',
            borderWidth: 1,
            titleColor: '#00d4ff',
            bodyColor: '#e0e6ed',
            titleFont: { family: 'Inter', size: 12, weight: '600' },
            bodyFont: { family: 'Inter', size: 11 },
            padding: 12,
            cornerRadius: 6,
            displayColors: true,
        },
        // Transparent plugin background (no white card behind charts)
        _darkBg: { backgroundColor: 'transparent' },
    },
    scales: {
        x: {
            grid: { color: 'rgba(0, 212, 255, 0.08)' },
            ticks: { color: '#8892a0', font: { family: 'Inter', size: 10 }, maxTicksLimit: 12 },
            title: { color: '#8892a0' },
        },
        y: {
            grid: { color: 'rgba(0, 212, 255, 0.08)' },
            ticks: { color: '#8892a0', font: { family: 'Inter', size: 10 } },
            title: { color: '#8892a0' },
        }
    }
};

// Store chart instances for cleanup
const chartInstances = {};

// ─── Crosshair Sync Plugin ──────────────────────────────────────────────────
// When hovering on one chart, shows matching crosshair on linked charts

const crosshairPlugin = {
    id: 'crosshairSync',
    afterEvent(chart, args) {
        const event = args.event;
        if (event.type === 'mousemove' && chart.options._syncGroup) {
            const group = chart.options._syncGroup;
            const xScale = chart.scales.x;
            if (!xScale) return;
            const idx = xScale.getValueForPixel(event.x);
            Object.entries(chartInstances).forEach(([id, c]) => {
                if (c !== chart && c.options._syncGroup === group && c.scales.x) {
                    c._syncIndex = Math.round(idx);
                    c.draw();
                }
            });
        }
        if (event.type === 'mouseout' && chart.options._syncGroup) {
            Object.entries(chartInstances).forEach(([id, c]) => {
                if (c !== chart && c.options._syncGroup === chart.options._syncGroup) {
                    c._syncIndex = null;
                    c.draw();
                }
            });
        }
    },
    afterDraw(chart) {
        if (chart._syncIndex != null && chart.scales.x) {
            const ctx = chart.ctx;
            const xScale = chart.scales.x;
            const yScale = chart.scales.y;
            const x = xScale.getPixelForValue(chart._syncIndex);
            if (x >= xScale.left && x <= xScale.right) {
                ctx.save();
                ctx.beginPath();
                ctx.setLineDash([4, 4]);
                ctx.strokeStyle = 'rgba(0, 212, 255, 0.4)';
                ctx.lineWidth = 1;
                ctx.moveTo(x, yScale.top);
                ctx.lineTo(x, yScale.bottom);
                ctx.stroke();
                ctx.restore();
            }
        }
    }
};

Chart.register(crosshairPlugin);

// ─── Crossfilter State ──────────────────────────────────────────────────────

let activeFilter = null;  // { type: 'eventType', value: 'Battles' }

function setCrossfilter(type, value) {
    activeFilter = { type, value };
    const bar = document.getElementById('crossfilterBar');
    const val = document.getElementById('crossfilterValue');
    if (bar && val) {
        val.textContent = value;
        bar.style.display = 'flex';
    }
    // Trigger re-render of overview charts with filter
    if (typeof renderOverviewFiltered === 'function') renderOverviewFiltered();
    if (typeof _updateStatusBar === 'function') _updateStatusBar();
}

function clearCrossfilter() {
    activeFilter = null;
    const bar = document.getElementById('crossfilterBar');
    if (bar) bar.style.display = 'none';
    if (typeof renderOverviewFiltered === 'function') renderOverviewFiltered();
    if (typeof _updateStatusBar === 'function') _updateStatusBar();
}

// Wire up clear button
document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('crossfilterClear')?.addEventListener('click', clearCrossfilter);
});

function destroyChart(id) {
    if (chartInstances[id]) {
        const canvas = chartInstances[id].canvas;
        chartInstances[id].destroy();
        delete chartInstances[id];
        // Clear stale inline styles left by Chart.js (critical for charts
        // created while their tab was hidden — they get stuck at 0×0)
        if (canvas) {
            canvas.removeAttribute('style');
            canvas.removeAttribute('width');
            canvas.removeAttribute('height');
        }
    }
}

/* ─── Tab 1: Executive Overview ──────────────────────────────────────────── */

function createPriceAttackChart(timeseries) {
    destroyChart('priceAttackChart');
    const ctx = document.getElementById('priceAttackChart');
    if (!ctx) return;

    const dates = timeseries.map(d => d.date);
    const prices = timeseries.map(d => d.brent_price);
    const attacks = timeseries.map(d => d.weekly_attacks);

    // If crossfilter active, highlight matching periods
    let attackColors = attacks.map(() => COLORS.attacksBg);
    let attackBorders = attacks.map(() => COLORS.attacks);
    if (activeFilter && activeFilter.type === 'eventType' && timeseries._eventDates) {
        const matchDates = new Set(timeseries._eventDates[activeFilter.value] || []);
        attackColors = dates.map(d => matchDates.has(d) ? 'rgba(224, 85, 85, 0.7)' : 'rgba(224, 85, 85, 0.08)');
        attackBorders = dates.map(d => matchDates.has(d) ? COLORS.attacks : 'rgba(224, 85, 85, 0.15)');
    }

    chartInstances['priceAttackChart'] = new Chart(ctx, {
        data: {
            labels: dates,
            datasets: [
                {
                    type: 'line',
                    label: 'Brent Price (USD/bbl)',
                    data: prices,
                    borderColor: COLORS.brent,
                    backgroundColor: COLORS.brentBg,
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    fill: true,
                    tension: 0.3,
                    yAxisID: 'y',
                    order: 1,
                },
                {
                    type: 'bar',
                    label: 'Weekly Attack Frequency',
                    data: attacks,
                    backgroundColor: attackColors,
                    borderColor: attackBorders,
                    borderWidth: 1,
                    yAxisID: 'y1',
                    order: 2,
                }
            ]
        },
        options: {
            ...CHART_DEFAULTS,
            _syncGroup: 'overview',
            interaction: { mode: 'index', intersect: false },
            scales: {
                x: { ...CHART_DEFAULTS.scales.x },
                y: {
                    ...CHART_DEFAULTS.scales.y,
                    position: 'left',
                    title: { display: true, text: 'Brent Price (USD/barrel)', font: { family: 'Inter', size: 11 } }
                },
                y1: {
                    ...CHART_DEFAULTS.scales.y,
                    position: 'right',
                    title: { display: true, text: 'Weekly Attacks', font: { family: 'Inter', size: 11 } },
                    grid: { drawOnChartArea: false },
                    beginAtZero: true,
                }
            }
        }
    });
}

function createVolatilityChart(timeseries) {
    destroyChart('volatilityChart');
    const ctx = document.getElementById('volatilityChart');
    if (!ctx) return;

    const dates = timeseries.map(d => d.date);
    const volatility = timeseries.map(d => d.daily_volatility);

    // If crossfilter active, highlight matching periods with a second dataset
    let highlightData = null;
    if (activeFilter && activeFilter.type === 'eventType' && timeseries._eventDates) {
        const matchDates = new Set(timeseries._eventDates[activeFilter.value] || []);
        highlightData = dates.map((d, i) => matchDates.has(d) ? volatility[i] : null);
    }

    const datasets = [{
        label: 'Daily Volatility',
        data: volatility,
        borderColor: activeFilter ? COLORS.volatility + '55' : COLORS.volatility,
        backgroundColor: activeFilter ? COLORS.volatilityBg + '33' : COLORS.volatilityBg,
        borderWidth: 2,
        pointRadius: 0,
        fill: true,
        tension: 0.3,
    }];

    if (highlightData) {
        datasets.push({
            label: `Volatility (${activeFilter.value})`,
            data: highlightData,
            borderColor: COLORS.attacks,
            backgroundColor: 'rgba(255, 82, 82, 0.25)',
            borderWidth: 2.5,
            pointRadius: 3,
            pointBackgroundColor: COLORS.attacks,
            fill: true,
            tension: 0.3,
            spanGaps: false,
        });
    }

    chartInstances['volatilityChart'] = new Chart(ctx, {
        type: 'line',
        data: { labels: dates, datasets },
        options: {
            ...CHART_DEFAULTS,
            _syncGroup: 'overview',
            scales: {
                x: { ...CHART_DEFAULTS.scales.x },
                y: {
                    ...CHART_DEFAULTS.scales.y,
                    title: { display: true, text: 'Volatility (% Daily Change)', font: { family: 'Inter', size: 11 } }
                }
            }
        }
    });
}

function createEventTypesChart(events) {
    destroyChart('eventTypesChart');
    const ctx = document.getElementById('eventTypesChart');
    if (!ctx) return;

    // Count event types
    const typeCounts = {};
    events.forEach(e => {
        const t = e.event_type || 'Unknown';
        typeCounts[t] = (typeCounts[t] || 0) + 1;
    });
    const sorted = Object.entries(typeCounts).sort((a, b) => b[1] - a[1]);
    const palette = [COLORS.brent, COLORS.attacks, COLORS.volatility, COLORS.dxy, COLORS.ovx, COLORS.positive, COLORS.gold];

    // Dim non-selected segments when crossfilter is active
    const bgColors = palette.slice(0, sorted.length).map((c, i) => {
        if (activeFilter && activeFilter.type === 'eventType' && sorted[i][0] !== activeFilter.value) {
            return c + '33'; // add transparency
        }
        return c;
    });

    chartInstances['eventTypesChart'] = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: sorted.map(s => s[0]),
            datasets: [{
                data: sorted.map(s => s[1]),
                backgroundColor: bgColors,
                borderColor: '#0d1420',
                borderWidth: 2,
            }]
        },
        options: {
            ...CHART_DEFAULTS,
            scales: {},
            onClick(e, elements) {
                if (elements.length > 0) {
                    const idx = elements[0].index;
                    const clickedType = sorted[idx][0];
                    if (activeFilter && activeFilter.value === clickedType) {
                        clearCrossfilter();
                    } else {
                        setCrossfilter('eventType', clickedType);
                    }
                }
            },
            plugins: {
                ...CHART_DEFAULTS.plugins,
                legend: { ...CHART_DEFAULTS.plugins.legend, position: 'right' },
            }
        }
    });
}

/* ─── Tab 3: Econometric Analysis ────────────────────────────────────────── */

function createModelComparisonChart(hypothesisData) {
    destroyChart('modelComparisonChart');
    const ctx = document.getElementById('modelComparisonChart');
    if (!ctx) return;

    const comp = hypothesisData.model_comparison;

    chartInstances['modelComparisonChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: comp.labels,
            datasets: [{
                label: 'R² Value',
                data: comp.r_squared,
                backgroundColor: [COLORS.brent, COLORS.attacks, COLORS.volatility],
                borderColor: [COLORS.brent, COLORS.attacks, COLORS.volatility],
                borderWidth: 1,
                borderRadius: 4,
            }]
        },
        options: {
            ...CHART_DEFAULTS,
            indexAxis: 'y',
            scales: {
                x: {
                    ...CHART_DEFAULTS.scales.x,
                    beginAtZero: true,
                    title: { display: true, text: 'R² (Explained Variance)', font: { family: 'Inter', size: 11 } }
                },
                y: { ...CHART_DEFAULTS.scales.y }
            },
            plugins: { ...CHART_DEFAULTS.plugins, legend: { display: false } }
        }
    });
}

function createPriceWindowChart(priceWindows) {
    destroyChart('priceWindowChart');
    const ctx = document.getElementById('priceWindowChart');
    if (!ctx) return;

    const labels = ['T-2', 'T-1', 'T0', 'T+1', 'T+2', 'T+3', 'T+4', 'T+5'];
    const keys = ['Price_T-2', 'Price_T-1', 'Price_T0', 'Price_T+1', 'Price_T+2', 'Price_T+3', 'Price_T+4', 'Price_T+5'];
    const data = keys.map(k => priceWindows[k] || 0);

    // Color the event day differently
    const bgColors = data.map((_, i) => i === 2 ? COLORS.attacks : COLORS.brent);
    const borderColors = data.map((_, i) => i === 2 ? '#ff6666' : '#6ba8e8');

    // Compute tight y-axis range so bars are visually distinct
    const validData = data.filter(v => v > 0);
    const minVal = Math.min(...validData);
    const maxVal = Math.max(...validData);
    const padding = (maxVal - minVal) * 0.5 || 0.5;
    const yMin = Math.floor((minVal - padding) * 10) / 10;
    const yMax = Math.ceil((maxVal + padding) * 10) / 10;

    chartInstances['priceWindowChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Avg Price (USD)',
                data: data,
                backgroundColor: bgColors,
                borderColor: borderColors,
                borderWidth: 1,
                borderRadius: 4,
                barPercentage: 0.7,
                categoryPercentage: 0.8,
            }]
        },
        options: {
            ...CHART_DEFAULTS,
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: { ...CHART_DEFAULTS.scales.x },
                y: {
                    ...CHART_DEFAULTS.scales.y,
                    beginAtZero: false,
                    min: yMin,
                    max: yMax,
                    title: { display: true, text: 'Price (USD/barrel)', font: { family: 'Inter', size: 11 }, color: '#8892a0' },
                    ticks: { ...CHART_DEFAULTS.scales.y.ticks, callback: v => '$' + v.toFixed(2) }
                }
            },
            plugins: { ...CHART_DEFAULTS.plugins, legend: { display: false } }
        }
    });
}

function createScatterChart(timeseries) {
    destroyChart('scatterChart');
    const ctx = document.getElementById('scatterChart');
    if (!ctx) return;

    const filtered = timeseries.filter(d => d.weekly_attacks > 0 && d.daily_volatility != null);

    // If crossfilter active, split into matching vs non-matching datasets
    const datasets = [];
    if (activeFilter && activeFilter.type === 'eventType' && timeseries._eventDates) {
        const matchDates = new Set(timeseries._eventDates[activeFilter.value] || []);
        const matching = filtered.filter(d => matchDates.has(d.date)).map(d => ({ x: d.weekly_attacks, y: d.daily_volatility }));
        const other = filtered.filter(d => !matchDates.has(d.date)).map(d => ({ x: d.weekly_attacks, y: d.daily_volatility }));
        datasets.push({
            label: activeFilter.value,
            data: matching,
            backgroundColor: COLORS.attacks,
            borderColor: COLORS.attacks,
            borderWidth: 1,
            pointRadius: 5,
            pointHoverRadius: 7,
        });
        datasets.push({
            label: 'Other Events',
            data: other,
            backgroundColor: 'rgba(136, 146, 160, 0.15)',
            borderColor: 'rgba(136, 146, 160, 0.3)',
            borderWidth: 1,
            pointRadius: 3,
            pointHoverRadius: 5,
        });
    } else {
        datasets.push({
            label: 'Attack Weeks',
            data: filtered.map(d => ({ x: d.weekly_attacks, y: d.daily_volatility })),
            backgroundColor: COLORS.attacksBg,
            borderColor: COLORS.attacks,
            borderWidth: 1,
            pointRadius: 4,
            pointHoverRadius: 6,
        });
    }

    chartInstances['scatterChart'] = new Chart(ctx, {
        type: 'scatter',
        data: { datasets },
        options: {
            ...CHART_DEFAULTS,
            scales: {
                x: {
                    ...CHART_DEFAULTS.scales.x,
                    title: { display: true, text: 'Weekly Attack Frequency', font: { family: 'Inter', size: 11 } }
                },
                y: {
                    ...CHART_DEFAULTS.scales.y,
                    title: { display: true, text: 'Daily Volatility (% Change)', font: { family: 'Inter', size: 11 } }
                }
            }
        }
    });
}

/* ─── Tab 4: Control Variables ───────────────────────────────────────────── */

function createDxyOvxChart(timeseries) {
    destroyChart('dxyOvxChart');
    const ctx = document.getElementById('dxyOvxChart');
    if (!ctx) return;

    const filtered = timeseries.filter(d => d.dxy != null || d.ovx != null);

    chartInstances['dxyOvxChart'] = new Chart(ctx, {
        data: {
            labels: filtered.map(d => d.date),
            datasets: [
                {
                    type: 'line',
                    label: 'DXY (US Dollar Index)',
                    data: filtered.map(d => d.dxy),
                    borderColor: COLORS.dxy,
                    backgroundColor: COLORS.dxyBg,
                    borderWidth: 2,
                    pointRadius: 0,
                    fill: false,
                    tension: 0.3,
                    yAxisID: 'y',
                },
                {
                    type: 'line',
                    label: 'OVX (Oil Volatility)',
                    data: filtered.map(d => d.ovx),
                    borderColor: COLORS.ovx,
                    backgroundColor: COLORS.ovxBg,
                    borderWidth: 2,
                    pointRadius: 0,
                    fill: false,
                    tension: 0.3,
                    yAxisID: 'y1',
                }
            ]
        },
        options: {
            ...CHART_DEFAULTS,
            interaction: { mode: 'index', intersect: false },
            scales: {
                x: { ...CHART_DEFAULTS.scales.x },
                y: {
                    ...CHART_DEFAULTS.scales.y,
                    position: 'left',
                    title: { display: true, text: 'DXY', font: { family: 'Inter', size: 11 } }
                },
                y1: {
                    ...CHART_DEFAULTS.scales.y,
                    position: 'right',
                    title: { display: true, text: 'OVX', font: { family: 'Inter', size: 11 } },
                    grid: { drawOnChartArea: false }
                }
            }
        }
    });
}

function createGeopoliticalChart(timeseries) {
    destroyChart('geopoliticalChart');
    const ctx = document.getElementById('geopoliticalChart');
    if (!ctx) return;

    chartInstances['geopoliticalChart'] = new Chart(ctx, {
        data: {
            labels: timeseries.map(d => d.date),
            datasets: [
                {
                    type: 'line',
                    label: 'Brent Price',
                    data: timeseries.map(d => d.brent_price),
                    borderColor: COLORS.brent,
                    borderWidth: 2,
                    pointRadius: 0,
                    tension: 0.3,
                    yAxisID: 'y',
                    order: 2,
                },
                {
                    type: 'bar',
                    label: 'OPEC Decisions',
                    data: timeseries.map(d => d.opec_dummy ? d.brent_price : null),
                    backgroundColor: COLORS.gold,
                    borderColor: COLORS.gold,
                    borderWidth: 0,
                    yAxisID: 'y',
                    order: 1,
                    barThickness: 3,
                },
                {
                    type: 'bar',
                    label: 'Russia-Ukraine',
                    data: timeseries.map(d => d.russia_ukraine_dummy ? -2 : null),
                    backgroundColor: 'rgba(123, 104, 174, 0.5)',
                    yAxisID: 'y1',
                    order: 1,
                    barThickness: 2,
                },
                {
                    type: 'bar',
                    label: 'Iran-Israel',
                    data: timeseries.map(d => d.iran_israel_escalation ? -1 : null),
                    backgroundColor: 'rgba(224, 123, 76, 0.5)',
                    yAxisID: 'y1',
                    order: 1,
                    barThickness: 2,
                }
            ]
        },
        options: {
            ...CHART_DEFAULTS,
            interaction: { mode: 'index', intersect: false },
            scales: {
                x: { ...CHART_DEFAULTS.scales.x },
                y: {
                    ...CHART_DEFAULTS.scales.y,
                    position: 'left',
                    title: { display: true, text: 'Brent Price (USD)', font: { family: 'Inter', size: 11 } }
                },
                y1: {
                    display: false,
                    position: 'right',
                    grid: { drawOnChartArea: false }
                }
            }
        }
    });
}

function createSprChart(timeseries) {
    destroyChart('sprChart');
    const ctx = document.getElementById('sprChart');
    if (!ctx) return;

    const filtered = timeseries.filter(d => d.spr_release_volume != null && d.spr_release_volume !== 0);

    chartInstances['sprChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: filtered.map(d => d.date),
            datasets: [{
                label: 'SPR Release Volume',
                data: filtered.map(d => d.spr_release_volume),
                backgroundColor: filtered.map(d => d.spr_release_volume >= 0 ? COLORS.positive : COLORS.negative),
                borderRadius: 2,
            }]
        },
        options: {
            ...CHART_DEFAULTS,
            plugins: { ...CHART_DEFAULTS.plugins, legend: { display: false } },
            scales: {
                x: { ...CHART_DEFAULTS.scales.x },
                y: {
                    ...CHART_DEFAULTS.scales.y,
                    title: { display: true, text: 'Volume Change (Million Barrels)', font: { family: 'Inter', size: 11 } }
                }
            }
        }
    });
}

/* ─── Tab 6: Current Events (US-Iran) ───────────────────────────────────── */

const IRAN_TYPE_COLORS = {
    military: '#E05555',
    diplomatic: '#4A90D9',
    sanctions: '#F09060',
    nuclear: '#9B8EC4',
    proxy: '#D4B870',
};

// ─── Iran Crossfilter State ─────────────────────────────────────────────────

let iranFilter = null; // { type: 'iranEventType', value: 'military' }

function setIranFilter(value) {
    iranFilter = { type: 'iranEventType', value };
    const bar = document.getElementById('iranCrossfilterBar');
    const val = document.getElementById('iranCrossfilterValue');
    if (bar && val) {
        val.textContent = value.charAt(0).toUpperCase() + value.slice(1);
        bar.style.display = 'flex';
    }
    if (typeof renderCurrentEventsFiltered === 'function') renderCurrentEventsFiltered();
    if (typeof _rerenderCuratedTimeline === 'function') _rerenderCuratedTimeline();
    updateIranIsraelMapLayers();
    if (typeof _updateStatusBar === 'function') _updateStatusBar();
}

function clearIranFilter() {
    iranFilter = null;
    const bar = document.getElementById('iranCrossfilterBar');
    if (bar) bar.style.display = 'none';
    if (typeof renderCurrentEventsFiltered === 'function') renderCurrentEventsFiltered();
    if (typeof _rerenderCuratedTimeline === 'function') _rerenderCuratedTimeline();
    updateIranIsraelMapLayers();
    if (typeof _updateStatusBar === 'function') _updateStatusBar();
}

document.getElementById('iranCrossfilterClear')?.addEventListener('click', clearIranFilter);

function createIranPriceTimelineChart(brentPrices, curatedEvents, zoomToWar = true) {
    destroyChart('iranPriceTimelineChart');
    const ctx = document.getElementById('iranPriceTimelineChart');
    if (!ctx || !brentPrices || !brentPrices.length) return;

    // Sort prices chronologically
    const sorted = [...brentPrices].sort((a, b) => a.date.localeCompare(b.date));

    // Filter based on zoom level
    const cutoff = zoomToWar ? '2026-01-15' : '2025-01-01';
    const filtered = sorted.filter(d => d.date >= cutoff);

    // Use labels array (like the working overview charts) so mode:'index' works
    const dates = filtered.map(d => d.date);
    const prices = filtered.map(d => d.price);

    // Build event lookup by date for scatter overlay + tooltip afterBody
    // Include curated, promoted ACLED, and geocoded news events
    const lastPrice = prices.length ? prices[prices.length - 1] : null;

    // Helper: find price on or before a given date (nearest trading day)
    function findNearestPrice(eventDate) {
        const exact = dates.indexOf(eventDate);
        if (exact >= 0) return { idx: exact, price: prices[exact], tradingDate: dates[exact] };
        // Binary search for the nearest trading day on or before this date
        let lo = 0, hi = dates.length - 1, best = -1;
        while (lo <= hi) {
            const mid = (lo + hi) >> 1;
            if (dates[mid] <= eventDate) { best = mid; lo = mid + 1; }
            else { hi = mid - 1; }
        }
        if (best >= 0) return { idx: best, price: prices[best], tradingDate: dates[best] };
        // If event is before all prices, use first price
        if (dates.length) return { idx: 0, price: prices[0], tradingDate: dates[0] };
        return { idx: -1, price: lastPrice, tradingDate: eventDate };
    }

    const eventPoints = (curatedEvents || []).filter(ev => ev.date >= cutoff).map(ev => {
        const match = findNearestPrice(ev.date);
        return {
            date: match.tradingDate,  // Snap to nearest trading day
            originalDate: ev.date,
            price: match.price,
            title: ev.title,
            type: ev.type,
            severity: ev.severity || 3,
            description: ev.description || '',
            source_type: ev.source_type || 'curated',
        };
    }).filter(p => p.price != null);

    // Deduplicate: if multiple events on same (snapped) date, keep highest severity (prefer curated)
    const eventByDate = {};
    eventPoints.forEach(p => {
        if (!eventByDate[p.date] || p.source_type === 'curated' || p.severity > (eventByDate[p.date].severity || 0)) {
            eventByDate[p.date] = p;
        }
    });

    // Build scatter data aligned to the labels axis (null for non-event dates)
    const scatterData = dates.map(d => eventByDate[d] ? eventByDate[d].price : null);

    // Color events by type — dim non-matching when filter active
    const eventColors = dates.map(d => {
        const ev = eventByDate[d];
        if (!ev) return 'transparent';
        const base = IRAN_TYPE_COLORS[ev.type] || COLORS.gold;
        if (iranFilter && iranFilter.value !== ev.type) return base + '33';
        return base;
    });
    const eventRadii = dates.map(d => {
        const ev = eventByDate[d];
        if (!ev) return 0;
        if (iranFilter && iranFilter.value !== ev.type) return 3;
        return 4 + ev.severity * 2;
    });

    chartInstances['iranPriceTimelineChart'] = new Chart(ctx, {
        data: {
            labels: dates,
            datasets: [
                {
                    type: 'line',
                    label: 'Brent Price (USD/bbl)',
                    data: prices,
                    borderColor: COLORS.brent,
                    backgroundColor: COLORS.brentBg,
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    fill: true,
                    tension: 0.3,
                    yAxisID: 'y',
                    order: 2,
                },
                {
                    type: 'line',
                    label: 'Major Events',
                    data: scatterData,
                    borderWidth: 0,
                    backgroundColor: eventColors,
                    borderColor: eventColors,
                    pointRadius: eventRadii,
                    pointHoverRadius: eventRadii.map(r => r > 0 ? 12 : 0),
                    pointStyle: 'circle',
                    showLine: false,
                    yAxisID: 'y',
                    order: 1,
                }
            ]
        },
        options: {
            ...CHART_DEFAULTS,
            interaction: { mode: 'index', intersect: false },
            scales: {
                x: {
                    ...CHART_DEFAULTS.scales.x,
                    type: 'time',
                    time: {
                        unit: zoomToWar ? 'week' : 'month',
                        displayFormats: { week: 'MMM d', month: 'MMM yyyy' },
                        tooltipFormat: 'MMM d, yyyy',
                    },
                    ticks: {
                        ...CHART_DEFAULTS.scales.x.ticks,
                        maxTicksLimit: zoomToWar ? 15 : 12,
                        autoSkip: true,
                    },
                    min: cutoff,
                },
                y: {
                    ...CHART_DEFAULTS.scales.y,
                    title: { display: true, text: 'Brent Price (USD/barrel)', font: { family: 'Inter', size: 11 } }
                }
            },
            plugins: {
                ...CHART_DEFAULTS.plugins,
                tooltip: {
                    ...CHART_DEFAULTS.plugins.tooltip,
                    filter(item) {
                        // Hide "Major Events" line from tooltip — show event info via afterBody
                        return item.datasetIndex === 0;
                    },
                    callbacks: {
                        label(context) {
                            return `Brent: $${context.parsed.y?.toFixed(2)}`;
                        },
                        afterBody(items) {
                            // Use the raw label (date string) via dataIndex, not the formatted tooltip label
                            const idx = items[0]?.dataIndex;
                            if (idx == null) return [];
                            const date = dates[idx];
                            const ev = eventByDate[date];
                            if (!ev) return [];
                            return ['', `⬤ ${ev.title}`, `Type: ${ev.type.toUpperCase()} | Severity: ${ev.severity}/5`];
                        }
                    }
                }
            }
        }
    });
}

/* ─── Price Forecast Chart ─────────────────────────────────────────────── */

function createIranForecastChart(brentPrices) {
    destroyChart('iranForecastChart');
    const ctx = document.getElementById('iranForecastChart');
    if (!ctx || !brentPrices || !brentPrices.length) return;

    // Sort chronologically then filter to recent
    const sorted = [...brentPrices].sort((a, b) => a.date.localeCompare(b.date));
    const recent = sorted.filter(d => d.date >= '2026-01-01');
    if (recent.length < 5) return;

    const lastPrice = recent[recent.length - 1].price;
    const lastDate = recent[recent.length - 1].date;

    // Historical dates and prices
    const histDates = recent.map(d => d.date);
    const histPrices = recent.map(d => d.price);

    // Generate 30-day forecast dates
    const forecastDays = 30;
    const forecastDates = [];
    const startDt = new Date(lastDate);
    for (let i = 1; i <= forecastDays; i++) {
        const d = new Date(startDt);
        d.setDate(d.getDate() + i);
        // Skip weekends
        if (d.getDay() === 0 || d.getDay() === 6) continue;
        forecastDates.push(d.toISOString().split('T')[0]);
    }

    // ── Dynamic scenario modeling ──
    // Scenarios are computed as percentage moves from the latest actual price.
    // Historical precedents:
    //   1990 Gulf War: Brent doubled ($20→$40) when 4.3 mbd disrupted
    //   1979 Iranian Revolution: +150% over 12 months
    //   2019 Aramco attack: +15% spike, recovered in weeks
    // Hormuz handles ~21 mbd (21% of global oil). Full closure = catastrophic.

    // Scenario ranges (% of lastPrice)
    const escGain = lastPrice * 0.40;   // +40% over 30 days (war widens)
    const susGain = lastPrice * 0.15;   // +15% over 30 days (slow grind)
    const ceaseDropAbs = lastPrice * 0.15; // -15% over 30 days (risk unwinds)

    // Compute 30-day target ranges for display
    const escLow  = Math.round(lastPrice + escGain * 0.7);
    const escHigh = Math.round(lastPrice + escGain);
    const susLow  = Math.round(lastPrice + susGain * 0.5);
    const susHigh = Math.round(lastPrice + susGain);
    const ceaseLow  = Math.round(lastPrice - ceaseDropAbs);
    const ceaseHigh = Math.round(lastPrice - ceaseDropAbs * 0.5);

    // Scenario 1: Escalation — War intensifies, Hormuz stays closed
    const escalation = forecastDates.map((d, i) => {
        const dayFrac = (i + 1) / forecastDates.length;
        const trend = lastPrice + (escGain * dayFrac) + (escGain * 0.08 * Math.sin(dayFrac * Math.PI * 3));
        return Math.round(trend * 100) / 100;
    });

    // Scenario 2: Sustained conflict — partial blockade, insurance-driven
    const sustained = forecastDates.map((d, i) => {
        const dayFrac = (i + 1) / forecastDates.length;
        const trend = lastPrice + (susGain * dayFrac) + (susGain * 0.1 * Math.sin(dayFrac * Math.PI * 2));
        return Math.round(trend * 100) / 100;
    });

    // Scenario 3: Ceasefire — Hormuz reopens, risk premium unwinds
    const deescalation = forecastDates.map((d, i) => {
        const dayFrac = (i + 1) / forecastDates.length;
        const trend = lastPrice - (ceaseDropAbs * dayFrac) + (ceaseDropAbs * 0.1 * Math.sin(dayFrac * Math.PI * 2));
        return Math.round(trend * 100) / 100;
    });

    // Update HTML scenario target labels with live-computed ranges
    const setEl = (id, text) => { const el = document.getElementById(id); if (el) el.textContent = text; };
    setEl('scenarioTargetEsc', `Target: $${escLow} — $${escHigh}/bbl`);
    setEl('scenarioTargetSus', `Target: $${susLow} — $${susHigh}/bbl`);
    setEl('scenarioTargetCease', `Target: $${ceaseLow} — $${ceaseHigh}/bbl`);

    // Chart legend labels with live ranges
    const escLabel = `Escalation ($${escLow}-$${escHigh})`;
    const susLabel = `Sustained Conflict ($${susLow}-$${susHigh})`;
    const ceaseLabel = `Ceasefire ($${ceaseLow}-$${ceaseHigh})`;

    // Combined labels: historical + forecast
    const allDates = [...histDates, ...forecastDates];
    const histLine = [...histPrices, ...new Array(forecastDates.length).fill(null)];
    const escLine = [...new Array(histDates.length - 1).fill(null), lastPrice, ...escalation];
    const susLine = [...new Array(histDates.length - 1).fill(null), lastPrice, ...sustained];
    const deescLine = [...new Array(histDates.length - 1).fill(null), lastPrice, ...deescalation];

    chartInstances['iranForecastChart'] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: allDates,
            datasets: [
                {
                    label: 'Actual Brent Price',
                    data: histLine,
                    borderColor: COLORS.brent,
                    backgroundColor: COLORS.brentBg,
                    borderWidth: 2.5,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    fill: false,
                    tension: 0.3,
                },
                {
                    label: escLabel,
                    data: escLine,
                    borderColor: '#C43D3D',
                    backgroundColor: 'rgba(196, 61, 61, 0.08)',
                    borderWidth: 2,
                    borderDash: [6, 3],
                    pointRadius: 0,
                    fill: false,
                    tension: 0.3,
                },
                {
                    label: susLabel,
                    data: susLine,
                    borderColor: '#E07B4C',
                    backgroundColor: 'rgba(224, 123, 76, 0.08)',
                    borderWidth: 2,
                    borderDash: [6, 3],
                    pointRadius: 0,
                    fill: false,
                    tension: 0.3,
                },
                {
                    label: ceaseLabel,
                    data: deescLine,
                    borderColor: '#2E7D5B',
                    backgroundColor: 'rgba(46, 125, 91, 0.08)',
                    borderWidth: 2,
                    borderDash: [6, 3],
                    pointRadius: 0,
                    fill: false,
                    tension: 0.3,
                },
            ]
        },
        options: {
            ...CHART_DEFAULTS,
            interaction: { mode: 'index', intersect: false },
            scales: {
                x: {
                    ...CHART_DEFAULTS.scales.x,
                    type: 'category',
                },
                y: {
                    ...CHART_DEFAULTS.scales.y,
                    title: { display: true, text: 'Brent Price (USD/barrel)', font: { family: 'Inter', size: 11 } },
                }
            },
            plugins: {
                ...CHART_DEFAULTS.plugins,
                legend: { ...CHART_DEFAULTS.plugins.legend, position: 'top' },
                tooltip: {
                    ...CHART_DEFAULTS.plugins.tooltip,
                    callbacks: {
                        label(context) {
                            const v = context.parsed.y;
                            return v != null ? `${context.dataset.label}: $${v.toFixed(2)}` : '';
                        }
                    }
                }
            }
        }
    });
}

function createIranEventTypeChart(iranEvents) {
    destroyChart('iranEventTypeChart');
    const ctx = document.getElementById('iranEventTypeChart');
    if (!ctx || !iranEvents || !iranEvents.length) return;

    const typeCounts = {};
    iranEvents.filter(e => (e.event_type || '').toLowerCase() !== 'protests').forEach(e => {
        const t = e.event_type || 'Unknown';
        typeCounts[t] = (typeCounts[t] || 0) + 1;
    });
    const sorted = Object.entries(typeCounts).sort((a, b) => b[1] - a[1]);
    const palette = [COLORS.attacks, COLORS.brent, COLORS.volatility, COLORS.dxy, COLORS.ovx, COLORS.positive, COLORS.gold];

    // Dim non-selected segments when crossfilter active
    const bgColors = palette.slice(0, sorted.length).map((c, i) => {
        if (iranFilter && iranFilter.value !== sorted[i][0].toLowerCase()) return c + '33';
        return c;
    });

    chartInstances['iranEventTypeChart'] = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: sorted.map(s => s[0]),
            datasets: [{
                data: sorted.map(s => s[1]),
                backgroundColor: bgColors,
                borderColor: '#0d1420',
                borderWidth: 2,
            }]
        },
        options: {
            ...CHART_DEFAULTS,
            scales: {},
            onClick(e, elements) {
                if (elements.length > 0) {
                    const idx = elements[0].index;
                    const clickedType = sorted[idx][0].toLowerCase();
                    if (iranFilter && iranFilter.value === clickedType) {
                        clearIranFilter();
                    } else {
                        setIranFilter(clickedType);
                    }
                }
            },
            plugins: {
                ...CHART_DEFAULTS.plugins,
                legend: { ...CHART_DEFAULTS.plugins.legend, position: 'right' },
            }
        }
    });
}

function createIranImpactChart(impactByType) {
    destroyChart('iranImpactChart');
    const ctx = document.getElementById('iranImpactChart');
    if (!ctx || !impactByType) return;

    const types = Object.keys(impactByType);
    if (!types.length) return;

    const horizons = ['T+1', 'T+3', 'T+5', 'T+7'];
    const horizonColors = ['rgba(74,144,217,0.7)', 'rgba(224,85,85,0.7)', 'rgba(240,192,80,0.7)', 'rgba(155,142,196,0.7)'];

    const datasets = horizons.map((h, i) => ({
        label: h,
        data: types.map(t => impactByType[t][h] || 0),
        backgroundColor: types.map(t => {
            if (iranFilter && iranFilter.value !== t) return horizonColors[i].replace('0.7', '0.15');
            return horizonColors[i];
        }),
        borderRadius: 3,
    }));

    chartInstances['iranImpactChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: types.map(t => t.charAt(0).toUpperCase() + t.slice(1)),
            datasets: datasets,
        },
        options: {
            ...CHART_DEFAULTS,
            scales: {
                x: { ...CHART_DEFAULTS.scales.x },
                y: {
                    ...CHART_DEFAULTS.scales.y,
                    title: { display: true, text: 'Avg Price Change (%)', font: { family: 'Inter', size: 11 } }
                }
            }
        }
    });
}

function createCorrelationChart(correlation) {
    destroyChart('correlationChart');
    const ctx = document.getElementById('correlationChart');
    if (!ctx || !correlation || !correlation.labels) return;

    const labels = correlation.labels;
    const matrix = correlation.matrix;
    const n = labels.length;

    // Use short labels
    const shortLabels = labels.map(l => {
        const map = {
            'Brent_Price': 'Brent',
            'Daily_Volatility': 'Volatility',
            'WeeklyAttackFreq': 'Attack Freq.',
            'OPEC_Dummy': 'OPEC',
            'RussiaUkraine_Dummy': 'Rus-Ukr',
            'IranIsrael_Escalation': 'Iran-Isr',
            'China_PMI': 'China PMI',
            'Baker_Hughes_Rigs': 'Rigs',
            'SPR_Release_Volume': 'SPR',
            'DXY': 'DXY',
            'OVX': 'OVX',
        };
        return map[l] || l;
    });

    // Build dataset for bubble-style heatmap
    const data = [];
    for (let i = 0; i < n; i++) {
        for (let j = 0; j < n; j++) {
            data.push({ x: j, y: i, v: matrix[i][j] });
        }
    }

    // Draw on canvas manually
    const canvas = ctx;
    const context = canvas.getContext('2d');
    const parent = canvas.parentElement;
    canvas.width = parent.clientWidth;
    canvas.height = parent.clientHeight;

    const cellW = Math.floor((canvas.width - 120) / n);
    const cellH = Math.floor((canvas.height - 60) / n);
    const offsetX = 120;
    const offsetY = 10;

    // Clear canvas with dark background
    context.fillStyle = 'transparent';
    context.fillRect(0, 0, canvas.width, canvas.height);

    // Draw cells — diverging colormap: Crimson (-1) → Dark (0) → Cyan (+1)
    data.forEach(d => {
        const val = d.v;
        let r, g, b;
        if (val >= 0) {
            // Dark base (20,30,48) → Cyan (0,212,255)
            r = Math.round(20 + val * (0 - 20));
            g = Math.round(30 + val * (212 - 30));
            b = Math.round(48 + val * (255 - 48));
        } else {
            // Dark base (20,30,48) → Crimson (224,85,85)
            const abs = Math.abs(val);
            r = Math.round(20 + abs * (224 - 20));
            g = Math.round(30 + abs * (85 - 30));
            b = Math.round(48 + abs * (85 - 48));
        }

        context.fillStyle = `rgb(${r},${g},${b})`;
        context.fillRect(offsetX + d.x * cellW, offsetY + d.y * cellH, cellW - 1, cellH - 1);

        // Cell border for definition
        context.strokeStyle = 'rgba(0, 212, 255, 0.1)';
        context.strokeRect(offsetX + d.x * cellW, offsetY + d.y * cellH, cellW - 1, cellH - 1);

        // Value text
        context.fillStyle = Math.abs(val) > 0.4 ? '#e0e6ed' : '#c0c8d4';
        context.font = 'bold 11px Inter, sans-serif';
        context.textAlign = 'center';
        context.textBaseline = 'middle';
        context.fillText(val.toFixed(2), offsetX + d.x * cellW + cellW / 2, offsetY + d.y * cellH + cellH / 2);
    });

    // Y-axis labels
    context.fillStyle = '#8892a0';
    context.font = '10px Inter';
    context.textAlign = 'right';
    context.textBaseline = 'middle';
    shortLabels.forEach((label, i) => {
        context.fillText(label, offsetX - 8, offsetY + i * cellH + cellH / 2);
    });

    // X-axis labels (rotated)
    context.save();
    shortLabels.forEach((label, j) => {
        context.save();
        context.translate(offsetX + j * cellW + cellW / 2, offsetY + n * cellH + 8);
        context.rotate(-Math.PI / 4);
        context.textAlign = 'right';
        context.fillStyle = '#8892a0';
        context.font = '10px Inter';
        context.fillText(label, 0, 0);
        context.restore();
    });
    context.restore();
}


/* ═══════════════════════════════════════════════════════════════════════════
   Maritime Traffic — Suez Canal Transit Volume Chart
   ═══════════════════════════════════════════════════════════════════════════ */

function createSuezTransitChart() {
    const canvas = document.getElementById('suezTransitChart');
    if (!canvas) return;

    // Destroy previous instance
    const existing = Chart.getChart(canvas);
    if (existing) existing.destroy();

    // Show loading state
    const ctx2d = canvas.getContext('2d');
    ctx2d.fillStyle = '#8892a0';
    ctx2d.font = '12px Inter';
    ctx2d.textAlign = 'center';
    ctx2d.fillText('Loading Suez Canal data from IMF PortWatch...', canvas.width / 2, canvas.height / 2);

    // Fetch live data from IMF PortWatch via backend
    fetch('/api/suez-transits')
        .then(resp => resp.json())
        .then(result => {
            const data = result.data;
            if (!data || !data.length) {
                ctx2d.clearRect(0, 0, canvas.width, canvas.height);
                ctx2d.fillText('No Suez Canal transit data available', canvas.width / 2, canvas.height / 2);
                return;
            }
            _renderSuezChart(canvas, data);
        })
        .catch(err => {
            console.error('Suez transit fetch failed:', err);
            ctx2d.clearRect(0, 0, canvas.width, canvas.height);
            ctx2d.fillStyle = COLORS.attacks;
            ctx2d.fillText('Failed to load Suez Canal data', canvas.width / 2, canvas.height / 2);
        });
}

function _renderSuezChart(canvas, data) {
    // Destroy any chart that was created during loading
    const existing = Chart.getChart(canvas);
    if (existing) existing.destroy();

    // Build datasets: total transits + tanker transits breakdown
    const totalData = data.map(d => ({ x: d.month + '-15', y: d.transits }));
    const tankerData = data.map(d => ({ x: d.month + '-15', y: d.tanker_transits }));

    // Dynamic y-axis minimum
    const minTransits = Math.min(...data.map(d => d.transits));
    const yMin = Math.max(0, Math.floor(minTransits * 0.8 / 100) * 100);

    // Build annotation lines — only show if data covers those dates
    const months = data.map(d => d.month);
    const annotations = {};

    if (months.some(m => m >= '2023-11')) {
        annotations.attackLine = {
            type: 'line',
            xMin: '2023-11-15',
            xMax: '2023-11-15',
            borderColor: COLORS.attacks,
            borderWidth: 2,
            borderDash: [6, 4],
            label: {
                display: true,
                content: 'Houthi Attacks Begin',
                position: 'start',
                backgroundColor: 'rgba(196, 61, 61, 0.85)',
                color: '#fff',
                font: { size: 10, family: 'Inter', weight: '600' },
                padding: 4,
            },
        };
    }

    if (months.some(m => m >= '2026-02')) {
        annotations.warLine = {
            type: 'line',
            xMin: '2026-02-15',
            xMax: '2026-02-15',
            borderColor: '#8B0000',
            borderWidth: 2,
            borderDash: [6, 4],
            label: {
                display: true,
                content: 'US-Iran War',
                position: 'start',
                backgroundColor: 'rgba(139, 0, 0, 0.85)',
                color: '#fff',
                font: { size: 10, family: 'Inter', weight: '600' },
                padding: 4,
            },
        };
    }

    new Chart(canvas, {
        type: 'line',
        data: {
            datasets: [
                {
                    label: 'Total Monthly Transits',
                    data: totalData,
                    borderColor: COLORS.brent,
                    backgroundColor: COLORS.brentBg,
                    fill: true,
                    tension: 0.3,
                    pointRadius: 3,
                    pointHoverRadius: 6,
                    borderWidth: 2.5,
                },
                {
                    label: 'Tanker Transits',
                    data: tankerData,
                    borderColor: COLORS.ovx,
                    backgroundColor: 'rgba(224, 123, 76, 0.10)',
                    fill: true,
                    tension: 0.3,
                    pointRadius: 2,
                    pointHoverRadius: 5,
                    borderWidth: 1.5,
                    borderDash: [4, 3],
                },
            ],
        },
        options: {
            ...CHART_DEFAULTS,
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'month',
                        displayFormats: { month: 'MMM yyyy' },
                        tooltipFormat: 'MMM yyyy',
                    },
                    grid: { display: false },
                    ticks: {
                        font: { size: 10, family: 'Inter' },
                        color: '#8892a0',
                        maxTicksLimit: 12,
                    },
                },
                y: {
                    beginAtZero: false,
                    min: yMin,
                    title: { display: true, text: 'Vessel Transits', font: { size: 11, family: 'Inter' }, color: '#8892a0' },
                    grid: { color: COLORS.gridLine },
                    ticks: {
                        font: { size: 10, family: 'Inter' },
                        color: '#8892a0',
                    },
                },
            },
            plugins: {
                ...CHART_DEFAULTS.plugins,
                tooltip: {
                    backgroundColor: '#0d1420',
                    borderColor: 'rgba(0, 212, 255, 0.3)',
                    borderWidth: 1,
                    titleColor: '#00d4ff',
                    bodyColor: '#e0e6ed',
                    titleFont: { family: 'Inter', weight: '600' },
                    bodyFont: { family: 'Inter' },
                    callbacks: {
                        label: (ctx) => `${ctx.dataset.label}: ${ctx.parsed.y.toLocaleString()} transits`,
                    },
                },
                annotation: { annotations },
            },
        },
    });
}


/* ═══════════════════════════════════════════════════════════════════════════
   Iran/Israel Geospatial Attack Map (Leaflet)
   ═══════════════════════════════════════════════════════════════════════════ */

let iranIsraelMap = null;
let iranIsraelHeatLayer = null;
let iranIsraelMarkerGroup = null;
let _iranMapData = { iran: [], israel: [], curated: [] };
let _iranTimeSlider = null;
let _iranTimeRange = null;

const IRAN_MAP_EVENT_COLORS = {
    'Battles': '#C43D3D',
    'Explosions/Remote violence': '#C43D3D',
    'Strategic developments': '#3D6B99',
    'Protests': '#E07B4C',
    'Riots': '#E07B4C',
    'Violence against civilians': '#7B68AE',
};

function initIranIsraelMap() {
    if (iranIsraelMap) return;

    iranIsraelMap = L.map('iranIsraelMap', {
        center: [32.0, 48.0],
        zoom: 5,
        zoomControl: true,
        attributionControl: true,
    });

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; <a href="https://carto.com/">CARTO</a>',
        subdomains: 'abcd',
        maxZoom: 19,
    }).addTo(iranIsraelMap);

    iranIsraelHeatLayer = L.heatLayer([], {
        radius: 25,
        blur: 20,
        maxZoom: 17,
        minOpacity: 0.25,
        gradient: { 0.1: '#0a2463', 0.3: '#00d4ff33', 0.5: '#00d4ff', 0.7: '#ffcc00', 0.85: '#ff4444', 1.0: '#ff0000' },
    });

    iranIsraelMarkerGroup = L.layerGroup().addTo(iranIsraelMap);

    // Key locations labels — tactical overlay
    const labels = [
        { pos: [26.5667, 56.2500], text: 'STRAIT OF HORMUZ', color: '#00d4ff' },
        { pos: [33.7225, 51.7275], text: 'NATANZ', color: '#ff6b6b' },
        { pos: [34.7564, 51.0596], text: 'FORDOW', color: '#ff6b6b' },
        { pos: [32.6546, 51.6680], text: 'ISFAHAN', color: '#ff6b6b' },
        { pos: [35.6892, 51.3890], text: 'TEHRAN', color: '#ffcc00' },
        { pos: [28.9684, 50.8385], text: 'BUSHEHR', color: '#ff6b6b' },
        { pos: [27.1865, 56.2808], text: 'BANDAR ABBAS', color: '#C9A96E' },
        { pos: [29.2333, 50.3167], text: 'KHARG ISLAND', color: '#C9A96E' },
        { pos: [32.0853, 34.7818], text: 'TEL AVIV', color: '#3D6B99' },
        { pos: [33.8938, 35.5018], text: 'BEIRUT', color: '#7B68AE' },
        { pos: [12.8, 43.3], text: 'BAB EL-MANDEB', color: '#00d4ff' },
    ];
    labels.forEach(l => {
        L.marker(l.pos, {
            icon: L.divIcon({
                className: 'chokepoint-label',
                html: `<div style="background:rgba(7,13,21,0.85);color:${l.color};padding:2px 6px;border:1px solid ${l.color}33;border-radius:2px;font-size:9px;font-weight:700;white-space:nowrap;font-family:'SF Mono','Fira Code',Consolas,monospace;position:relative;z-index:650;pointer-events:none;letter-spacing:1.5px">${l.text}</div>`,
                iconSize: [120, 16],
                iconAnchor: [60, 8],
            }),
            zIndexOffset: 1000,
            interactive: false,
        }).addTo(iranIsraelMap);
    });

    // Setup toggle controls — filter by event type
    ['toggleMilitary', 'toggleDiplomatic', 'toggleSanctions', 'toggleProxy', 'toggleIranHeatmap'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.addEventListener('change', () => updateIranIsraelMapLayers());
    });

    // Event type filter dropdown
    const typeFilter = document.getElementById('iranMapEventTypeFilter');
    if (typeFilter) typeFilter.addEventListener('change', () => updateIranIsraelMapLayers());
}

function loadIranIsraelMap(iranEvents, israelEvents, curatedEvents) {
    if (!iranIsraelMap) initIranIsraelMap();

    // Filter to war-relevant events only (exclude domestic protests/riots)
    const WAR_TYPES = new Set(['Battles', 'Explosions/Remote violence', 'Strategic developments', 'Violence against civilians']);
    _iranMapData.iran = (iranEvents || []).filter(e => e.latitude && e.longitude && WAR_TYPES.has(e.event_type)).map(e => ({
        lat: parseFloat(e.latitude),
        lng: parseFloat(e.longitude),
        date: (e.event_date || '').substring(0, 10),
        type: e.event_type || 'Unknown',
        subType: e.sub_event_type || '',
        actor: e.actor1 || '',
        location: e.location || '',
        notes: e.notes || '',
        fatalities: parseInt(e.fatalities) || 0,
        source: 'iran',
    }));

    _iranMapData.israel = (israelEvents || []).filter(e => e.latitude && e.longitude).map(e => ({
        lat: parseFloat(e.latitude),
        lng: parseFloat(e.longitude),
        date: (e.event_date || '').substring(0, 10),
        type: e.event_type || 'Unknown',
        subType: e.sub_event_type || '',
        actor: e.actor1 || '',
        location: e.location || '',
        notes: e.notes || '',
        fatalities: parseInt(e.fatalities) || 0,
        source: 'israel',
    }));

    _iranMapData.curated = (curatedEvents || []).filter(e => e.lat && e.lon).map(e => ({
        lat: e.lat,
        lng: e.lon,
        date: e.date,
        title: e.title,
        type: e.type,
        description: e.description || '',
        severity: e.severity || 1,
        location: e.location || '',
        fatalities: e.fatalities || 0,
        source: 'curated',
        source_type: e.source_type || 'curated',
    }));

    setupIranTimeSlider();
    updateIranIsraelMapLayers();
}

function setupIranTimeSlider() {
    const sliderEl = document.getElementById('iranTimeSlider');
    if (!sliderEl || _iranTimeSlider) return;

    const allDates = [
        ..._iranMapData.iran.map(e => e.date),
        ..._iranMapData.israel.map(e => e.date),
        ..._iranMapData.curated.map(e => e.date),
    ].filter(Boolean).sort();

    if (allDates.length < 2) return;

    const minTs = new Date(allDates[0]).getTime();
    const maxTs = new Date(allDates[allDates.length - 1]).getTime();
    _iranTimeRange = [minTs, maxTs];

    const fmtDate = ts => {
        const d = new Date(ts);
        return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    };

    noUiSlider.create(sliderEl, {
        start: [minTs, maxTs],
        connect: true,
        range: { min: minTs, max: maxTs },
        step: 86400000,
    });
    _iranTimeSlider = sliderEl.noUiSlider;

    const startLabel = document.getElementById('iranSliderStartDate');
    const endLabel = document.getElementById('iranSliderEndDate');

    _iranTimeSlider.on('update', (values) => {
        const s = parseInt(values[0]), e = parseInt(values[1]);
        if (startLabel) startLabel.textContent = fmtDate(s);
        if (endLabel) endLabel.textContent = fmtDate(e);
    });

    _iranTimeSlider.on('change', (values) => {
        _iranTimeRange = [parseInt(values[0]), parseInt(values[1])];
        updateIranIsraelMapLayers();
        // Propagate time filter to event table and curated timeline
        if (typeof renderCurrentEventsFiltered === 'function') renderCurrentEventsFiltered();
        if (typeof _rerenderCuratedTimeline === 'function') _rerenderCuratedTimeline();
        if (typeof _updateStatusBar === 'function') _updateStatusBar();
    });
}

// Map curated event types to filter categories
const _typeCategory = (type) => {
    const t = (type || '').toLowerCase();
    if (t === 'military' || t === 'battles' || t === 'explosions/remote violence' || t === 'violence against civilians') return 'military';
    if (t === 'diplomatic' || t === 'strategic developments') return 'diplomatic';
    if (t === 'sanctions' || t === 'nuclear') return 'sanctions';
    if (t === 'proxy') return 'proxy';
    return 'military'; // default
};

function updateIranIsraelMapLayers() {
    if (!iranIsraelMap) return;

    const showMilitary = document.getElementById('toggleMilitary')?.checked ?? true;
    const showDiplomatic = document.getElementById('toggleDiplomatic')?.checked ?? true;
    const showSanctions = document.getElementById('toggleSanctions')?.checked ?? true;
    const showProxy = document.getElementById('toggleProxy')?.checked ?? true;
    const showHeatmap = document.getElementById('toggleIranHeatmap')?.checked ?? true;

    const categoryVisible = { military: showMilitary, diplomatic: showDiplomatic, sanctions: showSanctions, proxy: showProxy };
    const passesTypeFilter = (type) => {
        const cat = _typeCategory(type);
        if (!(categoryVisible[cat] ?? true)) return false;
        // Also apply iranFilter crossfilter if active
        if (iranFilter && iranFilter.type === 'iranEventType') {
            return cat === _typeCategory(iranFilter.value);
        }
        return true;
    };

    // Time range filter
    const inTimeRange = (dateStr) => {
        if (!_iranTimeRange || !dateStr) return true;
        const ts = new Date(dateStr).getTime();
        return ts >= _iranTimeRange[0] && ts <= _iranTimeRange[1];
    };

    iranIsraelMarkerGroup.clearLayers();

    let allPoints = [];

    // ACLED events (Iran + Israel merged) — glowing tactical markers
    [..._iranMapData.iran, ..._iranMapData.israel].filter(e => passesTypeFilter(e.type) && inTimeRange(e.date)).forEach(e => {
        const color = IRAN_MAP_EVENT_COLORS[e.type] || '#636E72';
        const radius = Math.max(4, Math.min(12, 4 + e.fatalities * 0.5));
        const marker = L.circleMarker([e.lat, e.lng], {
            radius: radius,
            fillColor: color,
            color: color,
            weight: 1.5,
            fillOpacity: 0.55,
            opacity: 0.8,
        });
        // Outer glow ring for high-fatality events
        if (e.fatalities >= 3) {
            const glow = L.circleMarker([e.lat, e.lng], {
                radius: radius + 4,
                fillColor: color,
                color: color,
                weight: 0,
                fillOpacity: 0.15,
                interactive: false,
            });
            iranIsraelMarkerGroup.addLayer(glow);
        }
        marker.bindPopup(_buildMapPopup(e, e.source === 'israel' ? 'Israel ACLED' : 'Iran ACLED'));
        iranIsraelMarkerGroup.addLayer(marker);
        allPoints.push([e.lat, e.lng, Math.min(1.0, 0.4 + e.fatalities * 0.1)]);
    });

    // Curated + auto-discovered events (special markers)
    _iranMapData.curated.filter(e => passesTypeFilter(e.type) && inTimeRange(e.date)).forEach(e => {
        const srcType = e.source_type || 'curated';
        const pulseClass = srcType === 'acled_promoted' ? 'acled-promoted-pulse'
                         : srcType === 'news_auto' ? 'news-auto-pulse'
                         : 'curated-pulse';
        const dotSize = srcType === 'curated' ? 16 : 14;
        const dateColor = srcType === 'acled_promoted' ? '#4ECDC4'
                        : srcType === 'news_auto' ? '#3498DB'
                        : '#D4A843';
        const sourceBadge = srcType === 'acled_promoted'
            ? '<div style="font-size:9px;color:#fff;background:#4ECDC4;display:inline-block;padding:1px 5px;border-radius:3px;margin-bottom:6px">OSINT — ACLED</div>'
            : srcType === 'news_auto'
            ? '<div style="font-size:9px;color:#fff;background:#3498DB;display:inline-block;padding:1px 5px;border-radius:3px;margin-bottom:6px">OSINT — News</div>'
            : '';
        const marker = L.marker([e.lat, e.lng], {
            icon: L.divIcon({
                className: 'curated-pulse-wrap',
                html: `<div class="${pulseClass}" title="${_escChart(e.title)}"></div>`,
                iconSize: [dotSize, dotSize],
                iconAnchor: [dotSize/2, dotSize/2],
            }),
        });
        const h = _escChart;
        marker.bindPopup(`
            <div style="font-family:'SF Mono','Fira Code',Consolas,monospace;max-width:320px">
                <div style="font-weight:700;font-size:12px;margin-bottom:4px;color:${dateColor};letter-spacing:1px">${h(e.date)}</div>
                ${sourceBadge}
                <div style="font-weight:600;font-size:12px;margin-bottom:6px;color:#e0e8f0">${h(e.title)}</div>
                <div style="font-size:10px;margin-bottom:4px">
                    <span style="color:#5a6a7a">TYPE</span>
                    <span style="text-transform:uppercase;font-weight:500;color:#c0cad8;margin-left:6px">${h(e.type)}</span>
                </div>
                <div style="font-size:10px;margin-bottom:4px">
                    <span style="color:#5a6a7a">LOC</span>
                    <span style="color:#c0cad8;margin-left:6px">${h(e.location)}</span>
                </div>
                <div style="font-size:10px;margin-bottom:4px">
                    <span style="color:#5a6a7a">SEV</span>
                    <span style="color:#ffcc00;margin-left:6px">${'&#9733;'.repeat(e.severity)}</span>
                </div>${e.fatalities ? `
                <div style="font-size:10px;margin-bottom:4px">
                    <span style="color:#5a6a7a">KIA</span>
                    <strong style="color:#ff4444;margin-left:6px">${Number(e.fatalities).toLocaleString()}</strong>
                </div>` : ''}
                <div style="font-size:10px;color:#5a6a7a;margin-top:6px;line-height:1.4">${h(e.description)}</div>
            </div>
        `, { maxWidth: 340 });
        iranIsraelMarkerGroup.addLayer(marker);
        allPoints.push([e.lat, e.lng, srcType === 'curated' ? 1.0 : 0.7]);
    });

    // Heatmap
    if (showHeatmap && iranIsraelHeatLayer) {
        iranIsraelHeatLayer.setLatLngs(allPoints);
        if (!iranIsraelMap.hasLayer(iranIsraelHeatLayer)) iranIsraelMap.addLayer(iranIsraelHeatLayer);
    } else if (iranIsraelHeatLayer) {
        iranIsraelMap.removeLayer(iranIsraelHeatLayer);
    }

    // Update stats — count by type category
    const el = (id, val) => { const elem = document.getElementById(id); if (elem) elem.textContent = val; };
    const allEvts = [..._iranMapData.iran, ..._iranMapData.israel, ..._iranMapData.curated];
    const visibleEvts = allEvts.filter(e => passesTypeFilter(e.type) && inTimeRange(e.date));
    const militaryCount = visibleEvts.filter(e => _typeCategory(e.type) === 'military').length;
    const diplomaticCount = visibleEvts.filter(e => _typeCategory(e.type) === 'diplomatic').length;
    el('iranMapStatIran', militaryCount.toLocaleString());
    el('iranMapStatIsrael', diplomaticCount.toLocaleString());
    el('iranMapStatCurated', visibleEvts.length.toLocaleString());
    const totalFatalities = visibleEvts.reduce((s, e) => s + (e.fatalities || 0), 0);
    el('iranMapStatFatalities', totalFatalities.toLocaleString());
}

function _buildMapPopup(e, label) {
    const h = _escChart;
    const notesShort = (e.notes || '').substring(0, 200);
    const badgeColor = e.source === 'israel' ? '#3D6B99' : '#C43D3D';
    return `
        <div style="font-family:'SF Mono','Fira Code',Consolas,monospace;max-width:300px">
            <div style="font-weight:700;font-size:11px;margin-bottom:4px;color:#00d4ff;letter-spacing:1px">${h(e.date)}</div>
            <div style="font-size:9px;color:#fff;background:${badgeColor};display:inline-block;padding:2px 6px;border-radius:2px;margin-bottom:6px;letter-spacing:0.5px;text-transform:uppercase">${h(label)}</div>
            <div style="font-size:10px;margin-bottom:3px"><span style="color:#5a6a7a">TYPE</span> <span style="color:#c0cad8;margin-left:4px">${h(e.type)}</span></div>
            <div style="font-size:10px;margin-bottom:3px"><span style="color:#5a6a7a">SUB</span> <span style="color:#c0cad8;margin-left:4px">${h(e.subType || 'N/A')}</span></div>
            <div style="font-size:10px;margin-bottom:3px"><span style="color:#5a6a7a">ACTOR</span> <span style="color:#c0cad8;margin-left:4px">${h(e.actor || 'N/A')}</span></div>
            <div style="font-size:10px;margin-bottom:3px"><span style="color:#5a6a7a">LOC</span> <span style="color:#c0cad8;margin-left:4px">${h(e.location)}</span></div>
            <div style="font-size:10px;margin-bottom:3px"><span style="color:#5a6a7a">KIA</span> <strong style="color:${e.fatalities > 0 ? '#ff4444' : '#c0cad8'};margin-left:4px">${h(e.fatalities)}</strong></div>
            <div style="font-size:9px;color:#4a5a6a;margin-top:6px;line-height:1.4">${h(notesShort)}${(e.notes || '').length > 200 ? '...' : ''}</div>
        </div>
    `;
}

function resizeIranIsraelMap() {
    if (iranIsraelMap) {
        setTimeout(() => iranIsraelMap.invalidateSize(), 100);
    }
}
