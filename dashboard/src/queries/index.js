import { gql } from "@apollo/client";

export const GET_MONTHLY_KPIS = gql`
query GetMonthlyKPIs($year: Int!, $month: Int!) {
    getMonthlyKpis(year: $year, month: $month) {
    totalRevenue
    totalTrips
    avgTip
    dataTrustScore
    }
}
`;

export const GET_REGRESSION_METRICS = gql`
query GetRegressionMetrics($year: Int!, $month: Int!) {
    getRegressionMetrics(year: $year, month: $month) {
    accuracyPct
    dailyComparison {
        day
        avgReal
        avgPredicted
    }
    }
}
`;

export const GET_TIP_DISTRIBUTION = gql`
query GetTipDistribution($year: Int!, $month: Int!) {
    getTipDistribution(year: $year, month: $month) {
    highPct
    mediumPct
    lowPct
    }
}
`;

export const GET_TOP_ZONES = gql`
query GetTopZones($year: Int!, $month: Int!, $limit: Int!) {
    getTopZonesByRevenue(year: $year, month: $month, limit: $limit) {
    puLocationId
    totalRevenue
    }
}
`;

export const GET_ZONE_CLUSTERS = gql`
query GetZoneClusters {
    getZoneClusters {
    puLocationId
    clusterLabel
    avgRevenue
    avgDistance
    }
}
`;

export const GET_FORECAST = gql`
query GetForecast {
    getForecast {
    historical {
        date
        yhat
    }
    forecast {
        date
        yhat
        yhatLower
        yhatUpper
    }
    peakDays {
        date
        expectedTrips
    }
    }
}
`;