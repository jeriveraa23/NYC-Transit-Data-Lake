import { useState } from 'react'
import { useQuery } from '@apollo/client/react'
import {
LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
PieChart, Pie, Cell, BarChart, Bar, ScatterChart, Scatter, ZAxis,
} from 'recharts'
import {
GET_MONTHLY_KPIS,
GET_REGRESSION_METRICS,
GET_TIP_DISTRIBUTION,
GET_TOP_ZONES,
GET_ZONE_CLUSTERS,
} from '../queries/index'

const MONTHS = [
'January','February','March','April','May','June',
'July','August','September','October','November','December'
]

const CLUSTER_COLORS = ['#58a6ff', '#3fb950', '#d29922', '#f85149', '#bc8cff']

function fmt(n) {
if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(2)}M`
if (n >= 1_000)     return `$${(n / 1_000).toFixed(1)}K`
return `$${n.toFixed(2)}`
}

function fmtTrips(n) {
if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`
if (n >= 1_000)     return `${(n / 1_000).toFixed(0)}K`
return `${n}`
}

const CustomTooltip = ({ active, payload, label }) => {
if (!active || !payload?.length) return null
return (
    <div style={{ background: '#1c2128', border: '1px solid #30363d', borderRadius: 6, padding: '8px 12px', fontSize: 12 }}>
    <p style={{ color: '#8b949e', marginBottom: 4 }}>Day {label}</p>
    {payload.map(p => (
        <p key={p.name} style={{ color: p.color }}>{p.name}: ${p.value?.toFixed(2)}</p>
    ))}
    </div>
)
}

export default function Page1() {
const currentYear  = new Date().getFullYear()
const currentMonth = new Date().getMonth() + 1

const [year, setYear]   = useState(2024)
const [month, setMonth] = useState(1)

const vars = { year, month }

const { data: kpiData,     loading: l1 } = useQuery(GET_MONTHLY_KPIS,       { variables: vars })
const { data: regrData,    loading: l2 } = useQuery(GET_REGRESSION_METRICS,  { variables: vars })
const { data: tipData,     loading: l3 } = useQuery(GET_TIP_DISTRIBUTION,    { variables: vars })
const { data: zonesData,   loading: l4 } = useQuery(GET_TOP_ZONES,           { variables: { ...vars, limit: 10 } })
const { data: clusterData, loading: l5 } = useQuery(GET_ZONE_CLUSTERS)

const loading = l1 || l2 || l3 || l4 || l5

// KPI values
const kpis   = kpiData?.getMonthlyKpis
const regr   = regrData?.getRegressionMetrics
const tip    = tipData?.getTipDistribution
const zones  = zonesData?.getTopZonesByRevenue ?? []
const clusters = clusterData?.getZoneClusters ?? []

// Tip donut data
const tipChartData = tip ? [
    { name: 'Alta (>20%)',   value: tip.highPct,   color: '#3fb950' },
    { name: 'Media (5-20%)', value: tip.mediumPct, color: '#d29922' },
    { name: 'Baja (<5%)',    value: tip.lowPct,    color: '#f85149' },
] : []

// Top zones — sorted desc
const zonesChart = [...zones]
    .sort((a, b) => b.totalRevenue - a.totalRevenue)
    .map(z => ({ name: `Zone ${z.puLocationId}`, revenue: z.totalRevenue }))

// Cluster scatter — grouped by label
const clusterGroups = {}
clusters.forEach(z => {
    const lbl = z.clusterLabel
    if (!clusterGroups[lbl]) clusterGroups[lbl] = []
    clusterGroups[lbl].push({ x: z.avgRevenue, y: z.avgDistance })
})
const clusterLabels = Object.keys(clusterGroups)

const years = []
for (let y = 2023; y <= currentYear; y++) years.push(y)

return (
    <>
    {/* Header */}
    <div className="page-header">
        <div>
        <h1 className="page-title">Monthly Analytics</h1>
        <p className="page-subtitle">
            {MONTHS[month - 1]} {year} — NYC Yellow Taxi
        </p>
        </div>
        <div className="controls">
        <label>Year</label>
        <select className="select" value={year} onChange={e => setYear(+e.target.value)}>
            {years.map(y => <option key={y} value={y}>{y}</option>)}
        </select>
        <label>Month</label>
        <select className="select" value={month} onChange={e => setMonth(+e.target.value)}>
            {MONTHS.map((m, i) => <option key={i} value={i + 1}>{m}</option>)}
        </select>
        </div>
    </div>

    {/* KPI Cards */}
    <div className="kpi-grid">
        <div className="kpi-card">
        <p className="kpi-label">Total Revenue</p>
        <p className="kpi-value blue">{loading ? '—' : fmt(kpis?.totalRevenue ?? 0)}</p>
        <p className="kpi-desc">Sum of all fares in the month</p>
        </div>
        <div className="kpi-card">
        <p className="kpi-label">Total Trips</p>
        <p className="kpi-value white">{loading ? '—' : fmtTrips(kpis?.totalTrips ?? 0)}</p>
        <p className="kpi-desc">Trips processed this month</p>
        </div>
        <div className="kpi-card">
        <p className="kpi-label">Avg Tip</p>
        <p className="kpi-value green">{loading ? '—' : `$${(kpis?.avgTip ?? 0).toFixed(2)}`}</p>
        <p className="kpi-desc">Average tip per trip</p>
        </div>
        <div className="kpi-card">
        <p className="kpi-label">Data Trust Score</p>
        <p className="kpi-value amber">{loading ? '—' : `${(kpis?.dataTrustScore ?? 0).toFixed(1)}%`}</p>
        <p className="kpi-desc">Trips with consistent financials</p>
        </div>
        <div className="kpi-card">
        <p className="kpi-label">Regression Accuracy</p>
        <p className="kpi-value green">{loading ? '—' : `${(regr?.accuracyPct ?? 0).toFixed(1)}%`}</p>
        <p className="kpi-desc">Predictions within $2 of actual</p>
        </div>
    </div>

    {/* Charts 2x2 */}
    <div className="chart-grid">

        {/* Chart 1 — Regression */}
        <div className="chart-card">
        <p className="chart-title">Regression — Real vs Predicted</p>
        <p className="chart-subtitle">Daily average fare amount</p>
        {loading ? (
            <div className="loading-box">Loading...</div>
        ) : (
            <ResponsiveContainer width="100%" height={240}>
            <LineChart data={regr?.dailyComparison ?? []}>
                <CartesianGrid strokeDasharray="3 3" stroke="#21262d" />
                <XAxis dataKey="day" stroke="#6e7681" tick={{ fontSize: 11 }} label={{ value: 'Day', position: 'insideBottom', offset: -2, fontSize: 11, fill: '#6e7681' }} />
                <YAxis stroke="#6e7681" tick={{ fontSize: 11 }} tickFormatter={v => `$${v}`} />
                <Tooltip content={<CustomTooltip />} />
                <Legend wrapperStyle={{ fontSize: 12 }} />
                <Line type="monotone" dataKey="avgReal"      name="Real"      stroke="#58a6ff" dot={false} strokeWidth={2} />
                <Line type="monotone" dataKey="avgPredicted" name="Predicted" stroke="#d29922" dot={false} strokeWidth={2} strokeDasharray="4 4" />
            </LineChart>
            </ResponsiveContainer>
        )}
        </div>

        {/* Chart 2 — Tip Distribution */}
        <div className="chart-card">
        <p className="chart-title">Classification — Tip Distribution</p>
        <p className="chart-subtitle">% of trips by tip category this month</p>
        {loading ? (
            <div className="loading-box">Loading...</div>
        ) : (
            <ResponsiveContainer width="100%" height={240}>
            <PieChart>
                <Pie
                data={tipChartData}
                cx="50%" cy="50%"
                innerRadius={60} outerRadius={95}
                paddingAngle={3}
                dataKey="value"
                label={({ name, value }) => `${value.toFixed(1)}%`}
                labelLine={false}
                >
                {tipChartData.map((entry, i) => (
                    <Cell key={i} fill={entry.color} />
                ))}
                </Pie>
                <Legend
                formatter={(value, entry) => <span style={{ color: '#e6edf3', fontSize: 12 }}>{entry.payload.name}</span>}
                />
                <Tooltip formatter={(v) => `${v.toFixed(2)}%`} contentStyle={{ background: '#1c2128', border: '1px solid #30363d', fontSize: 12 }} />
            </PieChart>
            </ResponsiveContainer>
        )}
        </div>

        {/* Chart 3 — Top Zones */}
        <div className="chart-card">
        <p className="chart-title">Top 10 Zones by Revenue</p>
        <p className="chart-subtitle">Total revenue per pickup zone (gold layer)</p>
        {loading ? (
            <div className="loading-box">Loading...</div>
        ) : (
            <ResponsiveContainer width="100%" height={260}>
            <BarChart data={zonesChart} layout="vertical" margin={{ left: 10, right: 20 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#21262d" horizontal={false} />
                <XAxis type="number" stroke="#6e7681" tick={{ fontSize: 10 }} tickFormatter={v => `$${(v/1000).toFixed(0)}K`} />
                <YAxis type="category" dataKey="name" stroke="#6e7681" tick={{ fontSize: 10 }} width={62} />
                <Tooltip
                formatter={v => [`$${v.toLocaleString()}`, 'Revenue']}
                contentStyle={{ background: '#1c2128', border: '1px solid #30363d', fontSize: 12 }}
                />
                <Bar dataKey="revenue" fill="#58a6ff" radius={[0, 4, 4, 0]} />
            </BarChart>
            </ResponsiveContainer>
        )}
        </div>

        {/* Chart 4 — Cluster Scatter */}
        <div className="chart-card">
        <p className="chart-title">Clustering — Zone Segmentation</p>
        <p className="chart-subtitle">Avg revenue vs avg distance, colored by cluster (2-year history)</p>
        {loading ? (
            <div className="loading-box">Loading...</div>
        ) : (
            <ResponsiveContainer width="100%" height={260}>
            <ScatterChart margin={{ top: 10, right: 20, bottom: 10, left: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#21262d" />
                <XAxis type="number" dataKey="x" name="Avg Revenue" stroke="#6e7681" tick={{ fontSize: 10 }} tickFormatter={v => `$${v.toFixed(0)}`} label={{ value: 'Avg Revenue ($)', position:
'insideBottom', offset: -4, fontSize: 10, fill: '#6e7681' }} />
                <YAxis type="number" dataKey="y" name="Avg Distance" stroke="#6e7681" tick={{ fontSize: 10 }} tickFormatter={v => `${v.toFixed(1)}mi`} label={{ value: 'Avg Distance', angle: -90, position:
'insideLeft', offset: 10, fontSize: 10, fill: '#6e7681' }} />
                <ZAxis range={[20, 20]} />
                <Tooltip
                cursor={{ strokeDasharray: '3 3' }}
                contentStyle={{ background: '#1c2128', border: '1px solid #30363d', fontSize: 12 }}
                formatter={(v, name) => [name === 'Avg Revenue' ? `$${v.toFixed(2)}` : `${v.toFixed(2)} mi`, name]}
                />
                <Legend wrapperStyle={{ fontSize: 12 }} />
                {clusterLabels.map((label, i) => (
                <Scatter
                    key={label}
                    name={label}
                    data={clusterGroups[label]}
                    fill={CLUSTER_COLORS[i % CLUSTER_COLORS.length]}
                    fillOpacity={0.75}
                />
                ))}
            </ScatterChart>
            </ResponsiveContainer>
        )}
        </div>

    </div>
    </>
)
}