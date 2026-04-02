from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_cancellations",
    comment="Gold view of cancellations"
)
def gold_cancellations():
    return (
        spark.read.table("silver_streaming_ride_events")
        .filter(F.col("is_cancelled") == True)
        .groupBy("event_date", "city", "status", "cancellation_reason")
        .agg(
            F.count("ride_id").alias("ride_cancelleation_count"),
            F.round(F.avg("surge_multiplier"), 2).alias("avg_surge_at_cancell"),
            F.round(F.avg("distance_km"), 2).alias("avg_distance_km")
            )
        .orderBy(F.col("ride_cancelleation_count").desc())
        )