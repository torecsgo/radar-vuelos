import pyspark

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    LongType, BooleanType, IntegerType, ArrayType, FloatType
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import (
    from_json, split, col, timestamp_seconds, 
    timestamp_millis, date_format, when
)
import time

# Defining data structure
structure_flights = StructType([
    StructField("icao24", StringType(), True),              # Unique ICAO 24-bit address of the transponder
    StructField("callsign", StringType(), True),            # Callsign of the vehicle (Can be null)
    StructField("origin_country", StringType(), True),      # Country name inferred from the ICAO
    StructField("time_position", IntegerType(), True),      # Unix timestamp (seconds) for the last position update
    StructField("last_contact", IntegerType(), True),       # Unix timestamp (seconds) for the last update in general
    StructField("longitude", FloatType(), True),            # WGS-84 longitude in decimal degrees (Can be null)
    StructField("latitude", FloatType(), True),             # WGS-84 latitude in decimal degrees (Can be null)
    StructField("baro_altitude", FloatType(), True),        # Barometric altitude in meters (Can be null)
    StructField("on_ground", BooleanType(), True),          # True o False
    StructField("velocity", FloatType(), True),             # Velocity over ground in m/s (Can be null)
    StructField("true_track", FloatType(), True),           # True track in decimal degrees clockwise from north (Can be null)
    StructField("vertical_rate", FloatType(), True),        # Vertical rate in m/s Positive = Climbing; Negative = Descending (Can be null)
    StructField("sensors", ArrayType(IntegerType()), True), # IDs of the receivers which contributed to this state vector (Can be null)
    StructField("geo_altitude", FloatType(), True),         # Geometric altitude in meters (Can be null)
    StructField("squawk", StringType(), True),              # The transponder code aka Squawk (Can be null)
    StructField("spi", BooleanType(), True),                # Whether flight status indicates special purpose indicator
    StructField("position_source", IntegerType(), True)     # 0 = ADS-B; 1 = ASTERIX; 2 = MLAT; 3 = FLARM
])

# Suscribing to Kafka topic and Reading streaming data
spark = SparkSession.builder.appName("Radar_Flights").getOrCreate()
df_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "flights").option("startingOffsets", "earliest").load()

# TRANSORMING DATA
# Converting UNIX Time to String Date
# Adding flying_status col (To know where is the plane)
# Velocity in Km/h
# Removing columns

df_clean = df_stream \
    .selectExpr("CAST(value AS STRING) as flight_data") \
    .withColumn("datos_json", from_json(col("flight_data"), structure_flights)) \
    .select("datos_json.*") \
    .withColumn("time_position_date", timestamp_seconds(col("time_position"))) \
    .withColumn("last_contact_date", timestamp_seconds(col("last_contact"))) \
    .withColumn("flying_status",
        when(col("on_ground") == True, "On Ground")
        .when((col("baro_altitude") < 1000) & (col("vertical_rate") > 0), "Taking Off")
        .when((col("baro_altitude") < 1000) & (col("vertical_rate") < 0), "Landing")
        .otherwise("Flying")
    ) \
    .withColumn('velocity_km_h', col('velocity') * 3.6) \
    .drop("time_position", "last_contact", "on_ground", "velocity", "true_track", 
          "vertical_rate", "sensors", "geo_altitude", "squawk", "spi", "position_source")

query = df_clean.writeStream.format("memory").queryName("flight_data").start()

# SHOW DATA PROCESSED
time.sleep(5)
spark.sql("SELECT * FROM flight_data").show(truncate=False)