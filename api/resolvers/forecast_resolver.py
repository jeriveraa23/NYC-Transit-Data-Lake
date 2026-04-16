import pandas as pd
from datetime import date

def resolve_forecast(s3_service) -> dict:
    df = s3_service.read_parquet("ml/forecasts")
    df["ds"] = pd.to_datetime(df["ds"])
    df = df.sort_values("ds")

    today        = pd.Timestamp(date.today())
    historical_df = df[df["ds"] <= today]
    forecast_df   = df[df["ds"] >  today]

    def to_points(rows):
        return [
            {
                "date":       row["ds"].strftime("%Y-%m-%d"),
                "yhat":       int(row["yhat"]),
                "yhat_lower": int(row["yhat_lower"]),
                "yhat_upper": int(row["yhat_upper"])
            }
            for _, row in rows.iterrows()
        ]

    return {
        "historical": to_points(historical_df),
        "forecast":   to_points(forecast_df),
        "peak_days": [
            {
                "date":           row["ds"].strftime("%Y-%m-%d"),
                "expected_trips": int(row["yhat"])
            }
            for _, row in forecast_df.nlargest(5, "yhat").iterrows()
        ]
    }