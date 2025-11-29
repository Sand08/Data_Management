import json
import time
import datetime
import logging
import sys
import requests
import functions_framework
from google.cloud import pubsub_v1, storage

# --- Structured Logging for GCP ---
class GCPStructuredLogger(logging.Formatter):
    def format(self, record):
        log_entry = {
            "severity": record.levelname,
            "message": record.getMessage(),
            "timestamp": self.formatTime(record, self.datefmt),
        }
        return json.dumps(log_entry)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(GCPStructuredLogger())

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers = [handler]

# --- CONFIG ---
PROJECT_ID = "flightproject-479709"
TOPIC_ID = "opensky-flight-topic"
SUBSCRIPTION_ID = "opensky-flight-topic-sub"
BUCKET_NAME = "flightproject-479709-opensky"
GCS_PREFIX = "incoming-data/"
# --- Initialize GCP Clients ---
try:
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
except Exception as e:
    logger.error(f"[GCP Client Init Error] {e}")
    raise RuntimeError("GCP client setup failed.")

# --- Fetch State Vector Data ---
def fetch_state_vector_data():
    url = "https://opensky-network.org/api/states/all"
    try:
        response = requests.get(url, timeout=10)
        logger.info(f"[StateVector API] Status: {response.status_code}")
        if response.status_code == 200:
            return response.json().get("states", [])[:10]
    except Exception as e:
        logger.error(f"[StateVector API Error] {e}")
    return []

# --- Fetch Recent Flight Data ---
def fetch_flight_data():
    end_time = int(time.time())
    start_time = end_time - 3600
    url = f"https://opensky-network.org/api/flights/all?begin={start_time}&end={end_time}"
    try:
        response = requests.get(url, timeout=15)
        logger.info(f"[FlightData API] Status: {response.status_code}")
        if response.status_code == 200:
            return response.json()[:10]
    except Exception as e:
        logger.error(f"[FlightData API Error] {e}")
    return []

# --- Publish JSON to Pub/Sub ---
def publish_json(data_list, data_type):
    try:
        if data_type == "state_vector":
            wrapper = {
                "data_type": "flight_state",
                "time": int(time.time()),
                "states": [
                    [
                        d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7],
                        d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15], d[16]
                    ] for d in data_list if len(d) >= 17
                ]
            }
            future = publisher.publish(topic_path, json.dumps(wrapper).encode("utf-8"))
            future.result()
            logger.info(f"[PubSub] Published {len(wrapper['states'])} state_vector(s)")
        elif data_type == "flight_data":
            for d in data_list:
                d["data_type"] = data_type
                future = publisher.publish(topic_path, json.dumps(d).encode("utf-8"))
                future.result()
                logger.info(f"[PubSub] Published flight_data: {d.get('callsign') or d.get('icao24')}")
    except Exception as e:
        logger.error(f"[PubSub Error] Failed to publish {data_type}: {e}")

# --- Callback to Save to GCS ---
def callback(message):
    try:
        now = datetime.datetime.utcnow()
        filename = f"incoming-data/{now.strftime('%Y-%m-%dT%H-%M-%S-%f')}.json"
        blob = bucket.blob(filename)
        blob.upload_from_string(message.data, content_type='application/json')
        logger.info(f"[GCS] Saved message to {filename}")
        message.ack()
    except Exception as e:
        logger.error(f"[GCS Error] {e}")
        message.nack()

# --- Cloud Function Entry Point ---
@functions_framework.http
def main(request):
    logger.info("🚀 Triggered OpenSky ETL Cloud Function")

    try:
        # Step 1: Fetch + publish state vector
        state_vectors = fetch_state_vector_data()
        if state_vectors:
            publish_json(state_vectors, "state_vector")
        else:
            logger.warning("[StateVector] No data fetched")

        # Step 2: Fetch + publish recent flights
        flights = fetch_flight_data()
        if flights:
            publish_json(flights, "flight_data")
        else:
            logger.warning("[FlightData] No data fetched")

        logger.info("✅ Finished publishing to Pub/Sub")

        # Step 3: Subscribe & save to GCS for 60s
        logger.info(f"🕓 Subscribing to {subscription_path}")
        future = subscriber.subscribe(subscription_path, callback=callback)
        time.sleep(20)
        future.cancel()
        logger.info("Subscription cancelled after 20 seconds")

        return ("ETL pipeline completed successfully", 200)

    except Exception as e:
        logger.error(f"[Function Error] {e}")
        return (f"❌ Pipeline failed: {e}", 500)
