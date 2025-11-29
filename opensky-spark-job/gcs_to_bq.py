from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from google.cloud import storage
import logging
import sys
 
# Logging setup
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)
 
# Spark session
spark = SparkSession.builder \
    .appName("GCS to BigQuery Full Load - Auto Schema") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1") \
    .getOrCreate()
 
# Configs
bucket_name = "flightproject-479709-opensky"
gcs_prefix = "incoming-data/"
gcs_path = f"gs://{bucket_name}/{gcs_prefix}*.json"
project_id = "flightproject-479709"
dataset = "opensky_data"
flight_state_table = "flight_states_staging_new"
flight_data_table = "flight_data_staging_new"
 
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
 
try:
    logger.info(f"📥 Reading {gcs_path}")
    df = spark.read.option("multiline", "true").json(gcs_path)
    df.printSchema()
 
    # Flight state data (state_vector)
    logger.info("✈️ Processing state_vector data...")
    flight_state_df = df.filter(col("data_type") == "flight_state")
    exploded_df = flight_state_df.select(explode("states").alias("state"), "time", "data_type")
    flattened = exploded_df.selectExpr(
        "state[0] as icao24",
        "state[1] as callsign",
        "state[2] as origin_country",
        "state[3] as time_position",
        "state[4] as last_contact",
        "state[5] as longitude",
        "state[6] as latitude",
        "state[7] as baro_altitude",
        "state[8] as on_ground",
        "state[9] as velocity",
        "state[10] as true_track",
        "state[11] as vertical_rate",
        "state[12] as sensors",
        "state[13] as geo_altitude",
        "state[14] as squawk",
        "state[15] as spi",
        "state[16] as position_source",
        "time", "data_type"
    )
 
    flattened.write.format("bigquery") \
        .option("table", f"{project_id}.{dataset}.{flight_state_table}") \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save()
 
    logger.info("✅ state_vector written to BigQuery.")
 
    # Flight data
    logger.info("📦 Processing flight_data...")
    flight_data_df = df.filter(col("data_type") == "flight_data")
 
    # Drop nested column 'states' to avoid BigQuery schema issues
    if "states" in flight_data_df.columns:
        flight_data_df = flight_data_df.drop("states")
 
    flight_data_df.write.format("bigquery") \
        .option("table", f"{project_id}.{dataset}.{flight_data_table}") \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save()
 
    logger.info("✅ flight_data written to BigQuery.")
 
    # Delete processed files
    logger.info("🧹 Cleaning up processed files...")
    blobs = bucket.list_blobs(prefix=gcs_prefix)
    for blob in blobs:
        if blob.name.endswith(".json"):
            logger.info(f"🧹 Deleting processed file: {blob.name}")
            blob.delete()
 
except Exception as e:
    logger.error(f"❌ Error occurred: {e}")
 
finally:
    spark.stop() 