from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType



@dp.table(
    name = "silver_streaming_ride_events",
    comment = "Streaming taxi ride events",
    table_properties = {"quality": "silver"}
)


@dp.expect_or_drop("valid_ride_id", "ride_id IS NOT NULL")
@dp.expect_or_drop("valid_fare", "final_fare > 0")
@dp.expect_or_drop("valid_distance", "distance_km BETWEEN 0.5 AND 50")
@dp.expect_or_drop("valid_rating", "driver_rating BETWEEN 1.0 AND 5.0")
@dp.expect_or_drop(
    "valid_status",
    "status IN ('completed','cancelled_by_user','cancelled_by_driver','no_driver_found','in_progress')"
)
@dp.expect_or_drop(
    "valid_vehicle",
    "vehicle_type IN ('Bike','Auto','Mini','Sedan','SUV')"
)


def data_silver_ride_event():
    return (
        spark.readStream.table("bronze_streaming_ride_events")
        
        # Time transformations
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("event_hour", F.hour("event_time"))
        .withColumn("event_minute", F.minute("event_time"))
        .withColumn("day_of_week", F.dayofweek("event_time"))
        .withColumn(
            "is_weekend",
            F.dayofweek("event_time").isin([1, 7]).cast("boolean")
        )

        # Data type casting
        .withColumn("final_fare", F.col("final_fare").cast(DoubleType()))
        .withColumn("base_fare", F.col("base_fare").cast(DoubleType()))
        .withColumn("distance_km", F.col("distance_km").cast(DoubleType()))
        .withColumn("duration_mins", F.col("duration_mins").cast(IntegerType()))

        # Business logic
        .withColumn(
            "is_cancelled",
            F.col("status").isin(
                "cancelled_by_user",
                "cancelled_by_driver",
                "no_driver_found"
            ).cast("boolean")
        )

        # Cleanup
        .drop("_rescued_data")
    )