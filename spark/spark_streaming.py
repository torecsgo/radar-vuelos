import json
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, 
    IntegerType, ArrayType, FloatType
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, timestamp_seconds, when
)

import time

def main():
    with open("/app/data/generated-token.json", "r", encoding="utf-8") as f:
        auth_data = json.load(f)
        CLIENT_ID = auth_data["clientId"]
        CLIENT_SECRET = auth_data["secret"]

    spark = SparkSession.builder \
        .appName("Radar_Flights") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.config.cloud.path", "file:///app/data/secure-connect-radar-flights.zip") \
        .config("spark.cassandra.auth.username", CLIENT_ID) \
        .config("spark.cassandra.auth.password", CLIENT_SECRET) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    structure_flights = StructType([
        StructField("icao24", StringType(), True),
        StructField("callsign", StringType(), True),
        StructField("origin_country", StringType(), True),
        StructField("time_position", IntegerType(), True),
        StructField("last_contact", IntegerType(), True),
        StructField("longitude", FloatType(), True),
        StructField("latitude", FloatType(), True),
        StructField("baro_altitude", FloatType(), True),
        StructField("on_ground", BooleanType(), True),
        StructField("velocity", FloatType(), True),
        StructField("true_track", FloatType(), True),
        StructField("vertical_rate", FloatType(), True),
        StructField("sensors", ArrayType(IntegerType()), True),
        StructField("geo_altitude", FloatType(), True),
        StructField("squawk", StringType(), True),
        StructField("spi", BooleanType(), True),
        StructField("position_source", IntegerType(), True)
    ])

    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "flights") \
        .option("startingOffsets", "earliest") \
        .load()

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

    def write_to_cassandra(batch_df, batch_id):
        print(f"Processing batch {batch_id}...")

        df_flights = batch_df.drop("origin_country") \
            .filter(col("icao24").isNotNull() & col("time_position_date").isNotNull())

        df_flights.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="flights", keyspace="radar_flights") \
            .save()

        df_planes = batch_df.select("icao24", "callsign", "origin_country") \
            .filter(col("icao24").isNotNull() & col("callsign").isNotNull()) \
            .dropDuplicates(["icao24", "callsign"])

        df_planes.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="planes", keyspace="radar_flights") \
            .save()
            
        print(f"Batch {batch_id} saved in Cassandra (Astra DB).")

    query = df_clean.writeStream \
        .foreachBatch(write_to_cassandra) \
        .outputMode("update") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()