def resolve_tip_distribution(year: int, month: int, s3_service) -> dict:
    df = s3_service.read_parquet(
        "ml/predictions/classification",
        partitions={"year": year, "month": month},
        columns=["tip_label"]
    )

    total = len(df)
    counts = df["tip_label"].value_counts()

    high   = int(counts.get("Alta", 0))
    medium = int(counts.get("Media", 0))
    low    = int(counts.get("Baja", 0))

    return {
        "year": year,
        "month": month,
        "high_pct":   round(high   / total * 100, 2) if total > 0 else 0.0,
        "medium_pct": round(medium / total * 100, 2) if total > 0 else 0.0,
        "low_pct":    round(low    / total * 100, 2) if total > 0 else 0.0
    }