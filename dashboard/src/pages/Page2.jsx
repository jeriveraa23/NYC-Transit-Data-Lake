import { useQuery } from '@apollo/client/react'
import {
ComposedChart, Area, Line, XAxis, YAxis, CartesianGrid,
Tooltip, Legend, ResponsiveContainer,
} from 'recharts'
import { GET_FORECAST } from '../queries/index'

function formatDate(dateStr) {
const d = new Date(dateStr + 'T00:00:00')
return `${d.getMonth() + 1}/${d.getDate()}`
}

function formatDateLong(dateStr) {
const d = new Date(dateStr + 'T00:00:00')
return d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric', year: 'numeric' })
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
    <p style={{ color: '#8b949e', marginBottom: 4 }}>{label}</p>
    {payload.map((p, i) => p.value != null && (
        <p key={i} style={{ color: p.color ?? '#e6edf3' }}>
        {p.name}: {p.value?.toLocaleString()} trips
        </p>
    ))}
    </div>
)
}

export default function Page2() {
const { data, loading, error } = useQuery(GET_FORECAST)

// Build combined chart dataset
let chartData = []
let peakDays  = []

if (data) {
    const historical = data.getForecast.historical.slice(-90) // last 90 days
    const forecast   = data.getForecast.forecast
    peakDays         = [...data.getForecast.peakDays].sort((a, b) => b.expectedTrips - a.expectedTrips)

    const histPoints = historical.map(p => ({
    date: formatDate(p.date),
    historical: p.yhat,
    forecast: null,
    ciUpper: null,
    ciLower: null,
    }))

    const forePoints = forecast.map(p => ({
    date: formatDate(p.date),
    historical: null,
    forecast: p.yhat,
    ciUpper: p.yhatUpper,
    ciLower: p.yhatLower,
    }))

    chartData = [...histPoints, ...forePoints]
}

return (
    <>
    <div className="page-header" style={{ marginBottom: '0.5rem' }}>
        <div>
        <h1 className="page-title">Demand Forecast</h1>
        </div>
    </div>

    <p className="forecast-description">
        Using Meta Prophet, the model analyzes all available daily trip history and learns the overall
        trend, weekly seasonality, and annual seasonality. The chart shows the last 90 days of actual
        data (solid line) and the next 30 days of projected demand (dashed line) with a 95% confidence
        interval (shaded band).
    </p>

    {/* Forecast Chart */}
    <div className="chart-card" style={{ marginBottom: '1.25rem' }}>
        <p className="chart-title">Trip Demand — Historical + 30-Day Forecast</p>
        <p className="chart-subtitle">Daily trip count · Shaded area = 95% confidence interval</p>
        {loading && <div className="loading-box">Loading forecast data...</div>}
        {error  && <div className="error-box">Error loading forecast. Is the API running?</div>}
        {!loading && !error && (
        <ResponsiveContainer width="100%" height={340}>
            <ComposedChart data={chartData} margin={{ top: 10, right: 20, bottom: 20, left: 10 }}>
            <defs>
                <linearGradient id="ciGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%"  stopColor="#58a6ff" stopOpacity={0.15} />
                <stop offset="95%" stopColor="#58a6ff" stopOpacity={0.03} />
                </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#21262d" />
            <XAxis
                dataKey="date"
                stroke="#6e7681"
                tick={{ fontSize: 10 }}
                interval={14}
                label={{ value: 'Date', position: 'insideBottom', offset: -10, fontSize: 11, fill: '#6e7681' }}
            />
            <YAxis
                stroke="#6e7681"
                tick={{ fontSize: 10 }}
                tickFormatter={fmtTrips}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: 12, paddingTop: '8px' }} />

            {/* CI band */}
            <Area
                type="monotone"
                dataKey="ciUpper"
                name="CI Upper"
                fill="url(#ciGradient)"
                stroke="none"
                connectNulls={false}
                legendType="none"
            />
            <Area
                type="monotone"
                dataKey="ciLower"
                name="CI Lower"
                fill="#0d1117"
                stroke="none"
                connectNulls={false}
                legendType="none"
            />

            {/* Lines */}
            <Line
                type="monotone"
                dataKey="historical"
                name="Historical"
                stroke="#58a6ff"
                strokeWidth={2}
                dot={false}
                connectNulls={false}
            />
            <Line
                type="monotone"
                dataKey="forecast"
                name="Forecast"
                stroke="#d29922"
                strokeWidth={2}
                strokeDasharray="5 5"
                dot={false}
                connectNulls={false}
            />
            </ComposedChart>
        </ResponsiveContainer>
        )}
    </div>

    {/* Peak Days Table */}
    {!loading && !error && peakDays.length > 0 && (
        <div className="chart-card">
        <p className="chart-title" style={{ marginBottom: '1rem' }}>Projected Peak Days</p>
        <table className="peak-table">
            <thead>
            <tr>
                <th>#</th>
                <th>Date</th>
                <th>Expected Trips</th>
            </tr>
            </thead>
            <tbody>
            {peakDays.map((day, i) => (
                <tr key={day.date}>
                <td>
                    <span className={`rank ${i === 0 ? 'gold' : ''}`}>{i + 1}</span>
                </td>
                <td>{formatDateLong(day.date)}</td>
                <td style={{ color: '#58a6ff', fontWeight: 600 }}>
                    {day.expectedTrips.toLocaleString()}
                </td>
                </tr>
            ))}
            </tbody>
        </table>
        </div>
    )}
    </>
)
}