import pandas as pd

def resolve_regression_metrics(year: int, month: int, s3_service) -> dict:
    df = s3_service.read_parquet(
        "ml/predictions/regression",
        partitions={"year": year, "month": month},
        columns=["tpep_pickup_datetime", "total_amount", "predicted_total_amount"]
    )

    total = len(df)
    accurate = int(((df["total_amount"] - df["predicted_total_amount"]).abs() <= 2).sum())
    accuracy_pct = round(accurate / total * 100, 2) if total > 0 else 0.0

    df["day"] = pd.to_datetime(df["tpep_pickup_datetime"]).dt.day
    daily = (
        df.groupby("day")
        .agg(avg_real=("total_amount", "mean"), avg_predicted=("predicted_total_amount", "mean"))
        .reset_index()
        .sort_values("day")
    )

    return {
        "year": year,
        "month": month,
        "accuracy_pct": accuracy_pct,
        "daily_comparison": [
            {
                "day": int(row["day"]),
                "avg_real": round(float(row["avg_real"]), 2),
                "avg_predicted": round(float(row["avg_predicted"]), 2)
            }
            for _, row in daily.iterrows()
        ]
    }