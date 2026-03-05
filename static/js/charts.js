/* ═══════════════════════════════════════════════════════════════════════════
   Chart Definitions — All Chart.js configurations
   ═══════════════════════════════════════════════════════════════════════════ */

const COLORS = {
    brent: '#3D6B99',
    brentBg: 'rgba(61, 107, 153, 0.08)',
    attacks: '#C43D3D',
    attacksBg: 'rgba(196, 61, 61, 0.25)',
    volatility: '#D4A843',
    volatilityBg: 'rgba(212, 168, 67, 0.08)',
    positive: '#2E7D5B',
    negative: '#C43D3D',
    dxy: '#7B68AE',
    dxyBg: 'rgba(123, 104, 174, 0.08)',
    ovx: '#E07B4C',
    ovxBg: 'rgba(224, 123, 76, 0.08)',
    navy: '#1B2A4A',
    gold: '#C9A96E',
    gray: '#B2BEC3',
    gridLine: 'rgba(0,0,0,0.06)',
};

const CHART_DEFAULTS = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
        legend: {
            position: 'top',
            labels: {
                font: { family: 'Inter', size: 11, weight: '500' },
                padding: 16,
                usePointStyle: true,
                pointStyleWidth: 8,
            }
        },
        tooltip: {
            backgroundColor: 'rgba(27, 42, 74, 0.95)',
            titleFont: { family: 'Inter', size: 12, weight: '600' },
            bodyFont: { family: 'Inter', size: 11 },
            padding: 12,
            cornerRadius: 6,
            displayColors: true,
        }
    },
    scales: {
        x: {
            grid: { color: COLORS.gridLine },
            ticks: { font: { family: 'Inter', size: 10 }, maxTicksLimit: 12 }
        },
        y: {
            grid: { color: COLORS.gridLine },
            ticks: { font: { family: 'Inter', size: 10 } }
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
                ctx.strokeStyle = 'rgba(201, 169, 110, 0.6)';
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
}

function clearCrossfilter() {
    activeFilter = null;
    const bar = document.getElementById('crossfilterBar');
    if (bar) bar.style.display = 'none';
    if (typeof renderOverviewFiltered === 'function') renderOverviewFiltered();
}

// Wire up clear button
document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('crossfilterClear')?.addEventListener('click', clearCrossfilter);
});

function destroyChart(id) {
    if (chartInstances[id]) {
        chartInstances[id].destroy();
        delete chartInstances[id];
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
        attackColors = dates.map(d => matchDates.has(d) ? 'rgba(196, 61, 61, 0.7)' : 'rgba(196, 61, 61, 0.08)');
        attackBorders = dates.map(d => matchDates.has(d) ? COLORS.attacks : 'rgba(196, 61, 61, 0.15)');
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

    chartInstances['volatilityChart'] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: timeseries.map(d => d.date),
            datasets: [{
                label: 'Daily Volatility',
                data: timeseries.map(d => d.daily_volatility),
                borderColor: COLORS.volatility,
                backgroundColor: COLORS.volatilityBg,
                borderWidth: 2,
                pointRadius: 0,
                fill: true,
                tension: 0.3,
            }]
        },
        options: {
            ...CHART_DEFAULTS,
            _syncGroup: 'overview',
            scales: {
                x: { ...CHART_DEFAULTS.scales.x },
                y: {
                    ...CHART_DEFAULTS.scales.y,
                    title: { display: true, text: 'Volatility', font: { family: 'Inter', size: 11 } }
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
                borderColor: '#fff',
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
    const bgColors = data.map((_, i) => i === 2 ? COLORS.attacks : COLORS.navy);

    chartInstances['priceWindowChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Avg Price (USD)',
                data: data,
                backgroundColor: bgColors,
                borderRadius: 4,
            }]
        },
        options: {
            ...CHART_DEFAULTS,
            scales: {
                x: { ...CHART_DEFAULTS.scales.x },
                y: {
                    ...CHART_DEFAULTS.scales.y,
                    beginAtZero: false,
                    title: { display: true, text: 'Price (USD/barrel)', font: { family: 'Inter', size: 11 } }
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

    const points = timeseries
        .filter(d => d.weekly_attacks > 0 && d.daily_volatility != null)
        .map(d => ({ x: d.weekly_attacks, y: d.daily_volatility }));

    chartInstances['scatterChart'] = new Chart(ctx, {
        type: 'scatter',
        data: {
            datasets: [{
                label: 'Attack Weeks',
                data: points,
                backgroundColor: COLORS.attacksBg,
                borderColor: COLORS.attacks,
                borderWidth: 1,
                pointRadius: 4,
                pointHoverRadius: 6,
            }]
        },
        options: {
            ...CHART_DEFAULTS,
            scales: {
                x: {
                    ...CHART_DEFAULTS.scales.x,
                    title: { display: true, text: 'Weekly Attack Frequency', font: { family: 'Inter', size: 11 } }
                },
                y: {
                    ...CHART_DEFAULTS.scales.y,
                    title: { display: true, text: 'Daily Volatility', font: { family: 'Inter', size: 11 } }
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
                    data: timeseries.map(d => d.opec_decision ? d.brent_price : null),
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
                    data: timeseries.map(d => d.russia_ukraine_attacks ? -2 : null),
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
                    title: { display: true, text: 'Volume Change', font: { family: 'Inter', size: 11 } }
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
            'WeekleyAttackFrq': 'Attacks',
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

    // Clear canvas with white background
    context.fillStyle = '#FFFFFF';
    context.fillRect(0, 0, canvas.width, canvas.height);

    // Draw cells — diverging colormap: Crimson (-1) → White (0) → Navy (+1)
    data.forEach(d => {
        const val = d.v;
        let r, g, b;
        if (val >= 0) {
            // White (255,255,255) → Navy (27,42,74)
            r = Math.round(255 - val * (255 - 27));
            g = Math.round(255 - val * (255 - 42));
            b = Math.round(255 - val * (255 - 74));
        } else {
            // White (255,255,255) → Crimson (196,61,61)
            const abs = Math.abs(val);
            r = Math.round(255 - abs * (255 - 196));
            g = Math.round(255 - abs * (255 - 61));
            b = Math.round(255 - abs * (255 - 61));
        }

        context.fillStyle = `rgb(${r},${g},${b})`;
        context.fillRect(offsetX + d.x * cellW, offsetY + d.y * cellH, cellW - 1, cellH - 1);

        // Cell border for definition
        context.strokeStyle = 'rgba(200,200,200,0.3)';
        context.strokeRect(offsetX + d.x * cellW, offsetY + d.y * cellH, cellW - 1, cellH - 1);

        // Value text
        context.fillStyle = Math.abs(val) > 0.4 ? '#fff' : '#1B2A4A';
        context.font = 'bold 11px Inter, sans-serif';
        context.textAlign = 'center';
        context.textBaseline = 'middle';
        context.fillText(val.toFixed(2), offsetX + d.x * cellW + cellW / 2, offsetY + d.y * cellH + cellH / 2);
    });

    // Y-axis labels
    context.fillStyle = '#636E72';
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
        context.fillStyle = '#636E72';
        context.font = '10px Inter';
        context.fillText(label, 0, 0);
        context.restore();
    });
    context.restore();
}
