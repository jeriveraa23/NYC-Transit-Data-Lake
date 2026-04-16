import strawberry
import os
from typing import List

from api.services.s3_service import S3Service
from api.resolvers.kpi_resolver import resolve_monthly_kpis
from api.resolvers.regression_resolver import resolve_regression_metrics
from api.resolvers.classification_resolver import resolve_tip_distribution
from api.resolvers.gold_resolver import resolve_top_zones
from api.resolvers.clustering_resolver import resolve_zone_clusters
from api.resolvers.forecast_resolver import resolve_forecast

BUCKET_NAME = os.getenv("S3_BUCKET", "nyc-transit-data-lake")

def get_s3() -> S3Service:
    return S3Service(BUCKET_NAME)

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
    accuracy_pct: float
    daily_comparison: List[DailyRegression]

@strawberry.type
class TipDistribution:
    year: int
    month: int
    high_pct: float
    medium_pct: float
    low_pct: float

@strawberry.type
class ZoneRevenue:
    pu_location_id: int
    total_revenue: float

@strawberry.type
class ZoneCluster:
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

@strawberry.type
class Query:

    @strawberry.field
    def get_monthly_kpis(self, year: int, month: int) -> MonthlyKPIs:
        return MonthlyKPIs(**resolve_monthly_kpis(year, month, get_s3()))

    @strawberry.field
    def get_regression_metrics(self, year: int, month: int) -> RegressionMetrics:
        data = resolve_regression_metrics(year, month, get_s3())
        daily = [DailyRegression(**d) for d in data.pop("daily_comparison")]
        return RegressionMetrics(**data, daily_comparison=daily)

    @strawberry.field
    def get_tip_distribution(self, year: int, month: int) -> TipDistribution:
        return TipDistribution(**resolve_tip_distribution(year, month, get_s3()))

    @strawberry.field
    def get_top_zones_by_revenue(self, year: int, month: int, limit: int = 10) -> List[ZoneRevenue]:
        return [ZoneRevenue(**z) for z in resolve_top_zones(year, month, limit, get_s3())]

    @strawberry.field
    def get_zone_clusters(self) -> List[ZoneCluster]:
        return [ZoneCluster(**c) for c in resolve_zone_clusters(get_s3())]

    @strawberry.field
    def get_forecast(self) -> ForecastData:
        data = resolve_forecast(get_s3())
        return ForecastData(
            historical=[ForecastPoint(**p) for p in data["historical"]],
            forecast=[ForecastPoint(**p) for p in data["forecast"]],
            peak_days=[PeakDay(**p) for p in data["peak_days"]]
        )


schema = strawberry.Schema(query=Query)