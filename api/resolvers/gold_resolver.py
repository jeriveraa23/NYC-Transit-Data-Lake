def resolve_top_zones(year: int, month: int, limit: int, s3_service) -> list:
    df = s3_service.read_parquet(
        "gold",
        partitions={"year": year, "month": month},
        columns=["PULocationID", "total_revenue"]
    )
    top = (
        df.groupby("PULocationID")["total_revenue"]
        .sum()
        .reset_index()
        .sort_values("total_revenue", ascending=False)
        .head(limit)
    )
    return [
        {
            "pu_location_id": int(row["PULocationID"]),
            "total_revenue":  round(float(row["total_revenue"]), 2)
        }
        for _, row in top.iterrows()
    ]