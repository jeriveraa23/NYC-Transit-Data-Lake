def resolve_zone_clusters(s3_service) -> list:
    df = s3_service.read_parquet("ml/clusters")

    return [
        {
            "pu_location_id":       int(row["PULocationID"]),
            "cluster":              int(row["cluster"]),
            "cluster_label":        str(row["cluster_label"]),
            "avg_revenue":          round(float(row["avg_revenue"]), 2),
            "avg_distance":         round(float(row["avg_distance"]), 2),
            "avg_duration_minutes": round(float(row["avg_duration_minutes"]), 2),
            "avg_tip_efficiency":   round(float(row["avg_tip_efficiency"]), 2),
            "total_trips":          int(row["total_trips"])
        }
        for _, row in df.iterrows()
    ]