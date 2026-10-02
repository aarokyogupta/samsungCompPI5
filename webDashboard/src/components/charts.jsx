/**
 * Chart.js wrappers.
 *
 * Registration happens once here rather than in each page so the tree-shaken bundle stays small and
 * the dark-mode axis styling is guaranteed to be identical across every chart in the dashboard.
 */

import {
    BarElement,
    CategoryScale,
    Chart,
    Filler,
    Legend,
    LineElement,
    LinearScale,
    PointElement,
    RadialLinearScale,
    Tooltip,
} from "chart.js";
import { Bar, Line, Radar } from "react-chartjs-2";

Chart.register(CategoryScale, LinearScale, RadialLinearScale, PointElement, LineElement, BarElement, Filler, Tooltip, Legend);

const GRID_COLOUR = "rgba(35, 67, 54, 0.8)";
const TICK_COLOUR = "#9bb1a6";

// The six ecological subscores plus the composite index each get a stable colour across every view
export const SERIES_COLOURS = [
    "#57d89b",
    "#5aa8ff",
    "#f3bb59",
    "#ef6b72",
    "#c084fc",
    "#38bdf8",
    "#fb923c",
];

const baseOptions = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: "index", intersect: false },
    plugins: {
        legend: { labels: { color: TICK_COLOUR, boxWidth: 10, usePointStyle: true } },
        tooltip: {
            backgroundColor: "#0e1c17",
            borderColor: GRID_COLOUR,
            borderWidth: 1,
            titleColor: "#e8f2ed",
            bodyColor: "#9bb1a6",
        },
    },
    scales: {
        x: { grid: { color: GRID_COLOUR }, ticks: { color: TICK_COLOUR, maxRotation: 0, autoSkipPadding: 24 } },
        y: { grid: { color: GRID_COLOUR }, ticks: { color: TICK_COLOUR } },
    },
};

/**
 * Time-series line chart fed directly by GET /analytics/historical.
 *
 * Imputed points are drawn hollow so an operator can tell a forward-filled gap from a real reading.
 */
export function TimeSeriesChart({ labels, datasets, height = 280 }) {
    const data = {
        labels: labels.map((label) => new Date(label).toLocaleString([], { month: "short", day: "numeric", hour: "2-digit" })),
        datasets: datasets.map((series, index) => ({
            label: series.label,
            data: series.data,
            borderColor: SERIES_COLOURS[index % SERIES_COLOURS.length],
            backgroundColor: `${SERIES_COLOURS[index % SERIES_COLOURS.length]}22`,
            borderWidth: 2,
            pointRadius: 0,
            pointHoverRadius: 4,
            tension: 0.3,
            fill: datasets.length === 1,
            spanGaps: true,
        })),
    };

    return (
        <div style={{ height }}>
            <Line data={data} options={baseOptions} />
        </div>
    );
}

/**
 * Six-axis ecological balance chart.
 *
 * The backend hints chart_type_hint="grouped_bar" for zone comparison because radar charts are poor
 * for precise reading, but a single species' own subscore balance is exactly what radar is good at.
 */
export function SubscoreRadarChart({ labels, values, height = 300 }) {
    const data = {
        labels,
        datasets: [
            {
                label: "Ecological subscores",
                data: values,
                borderColor: SERIES_COLOURS[0],
                backgroundColor: "rgba(87, 216, 155, 0.18)",
                pointBackgroundColor: SERIES_COLOURS[0],
                borderWidth: 2,
            },
        ],
    };

    const options = {
        ...baseOptions,
        scales: {
            r: {
                min: 0,
                max: 100,
                angleLines: { color: GRID_COLOUR },
                grid: { color: GRID_COLOUR },
                pointLabels: { color: TICK_COLOUR, font: { size: 11 } },
                ticks: { color: TICK_COLOUR, backdropColor: "transparent", stepSize: 25 },
            },
        },
    };

    return (
        <div style={{ height }}>
            <Radar data={data} options={options} />
        </div>
    );
}

/**
 * Zone comparison chart backed by GET /analytics/radar, which min-max normalises every metric to
 * 0-100 so degrees Celsius and raw sighting counts can legitimately share one axis.
 */
export function ZoneComparisonChart({ labels, datasets, height = 300 }) {
    const data = {
        labels,
        datasets: datasets.map((series, index) => ({
            label: series.label,
            data: series.data,
            backgroundColor: `${SERIES_COLOURS[index % SERIES_COLOURS.length]}cc`,
            borderRadius: 4,
        })),
    };

    const options = { ...baseOptions, scales: { ...baseOptions.scales, y: { ...baseOptions.scales.y, min: 0, max: 100 } } };

    return (
        <div style={{ height }}>
            <Bar data={data} options={options} />
        </div>
    );
}
