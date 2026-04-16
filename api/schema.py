import strawberry
import os
from typing import List, Optional



@strawberry.type
class MonthlyKPIs:
    year: int
    month: int
    total_revenue: float
    total_trips: int
    avg_tip: float
    data_trust_score: float

@strawberry.type
class DailyRegression:
    day: int
    avg_real: float
    avg_predicted: float

@strawberry.type
class RegressionMetrics:
    year: int
    month: int
    accuary_pct: float
    daily_comparision: List[DailyRegression]

@strawberry.type
class TipDistribution:
    year: int
    month: int
    high_pct: float
    medium_pct: float
    low_pct: float

@strawberry.type
class ZonaRevenue:
    pu_location_id: int
    total_revenue: float

@strawberry.type
class ZonaCluster:
    pu_location_id: int
    cluster: int
    cluster_label: str
    avg_revenue: float
    avg_distance: float
    avg_duration_minutes: float
    avg_tip_efficiency: float
    total_trips: int

@strawberry.type
class ForecastPoint:
    date: str
    yhat: int
    yhat_lower: int
    yhat_upper: int 

@strawberry.type
class PeakDay:
    date: str
    expected_trips: int

@strawberry.type
class ForecastData:
    historical: List[ForecastPoint]
    forecast: List[ForecastPoint]
    peak_days: List[PeakDay]

class Query:

    @strawberry.field
    def get_monthly_kpis(self, year: int, month: int) -> MonthlyKPIs:
        return MonthlyKPIs(**resolve_monthly_kpis())

schema = strawberry.Schema(query=Query)