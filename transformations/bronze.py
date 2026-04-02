from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

LANDING_PATH = spark.conf.get("LANDING_PATH")



RIDE_SCHEMA = StructType([
    StructField("ride_id", StringType(), nullable=True),
    StructField("event_time", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("pickup_area", StringType(), nullable=True),
    StructField("drop_area", StringType(), nullable=True),
    StructField("vehicle_type", StringType(), nullable=True),
    StructField("driver_id", StringType(), nullable=True),
    StructField("driver_name", StringType(), nullable=True),
    StructField("driver_rating", DoubleType(), nullable=True),
    StructField("rider_id", StringType(), nullable=True),
    StructField("distance_km", StringType(), nullable=True),
    StructField("duration_mins", IntegerType(), nullable=True),
    StructField("base_fare", DoubleType(), nullable=True),
    StructField("surge_multiplier", DoubleType(), nullable=True),
    StructField("final_fare", DoubleType(), nullable=True),
    StructField("payment_method", StringType(), nullable=True),
    StructField("cancellation_reason", StringType(), nullable=True),
    StructField("zone_id", StringType(), nullable=True),
    StructField("is_peak_hour", BooleanType(), nullable=True)
])


@dp.table(
  name = "bronze_streaming_ride_events",
  comment = "Streaming data from the landing zone",
  table_properties={"quality": "bronze"}
)
def bronze_streaming_ride_data():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(RIDE_SCHEMA)
        .load(LANDING_PATH)
    ).withColumn("_ingest_time", F.current_timestamp())




