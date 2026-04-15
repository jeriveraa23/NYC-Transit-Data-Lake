import strawberry
from typing import List, Optional

@strawberry.type
class TripMetrics:
    year: int
    month: int
    total_trips: int
    total_amount: float
    avg_fare: float

@strawberry.type
class Query:

    @strawberry.field
    def get_trip_metrics(self, year: int, month: int) -> Optional[TripMetrics]:
        return TripMetrics(year=year, month=month, total_trips=10000, total_amount=250000.0, avg_fare=25.0)

schema = strawberry.Schema(query=Query)