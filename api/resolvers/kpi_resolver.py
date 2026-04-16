def resolve_monthly_kpis(year: int, month: int, s3_service) -> dict:
    df = s3_service.read_parquet(
        "gold",
        partitions={"year": year, "month": month},
        columns=["total_trips", "total_revenue", "total_tips_profit", "flagged_incorrect_trips"]
    )

    total_trips   = int(df["total_trips"].sum())
    total_revenue = float(df["total_revenue"].sum())
    total_tips    = float(df["total_tips_profit"].sum())
    flagged       = float(df["flagged_incorrect_trips"].sum())

    return {
        "year": year,
        "month": month,
        "total_revenue": round(total_revenue, 2),
        "total_trips": total_trips,
        "avg_tip": round(total_tips / total_trips, 2) if total_trips > 0 else 0.0,
        "data_trust_score": round((1 - flagged / total_trips) * 100, 2) if total_trips > 0 else 0.0
    }

