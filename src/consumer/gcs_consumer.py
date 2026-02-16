import json
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Consumer
from google.cloud import storage # This uses your ADC automatically!

load_dotenv()

# --- CONFIGURATION ---
TOPIC_NAME = 'athlete_telemetry'
BUCKET_NAME = 'telemetry-lake-sagar' # <--- CHANGE THIS TO YOUR BUCKET NAME
BATCH_SIZE = 10  # Upload a file every 10 messages (for testing)

# --- GOOGLE CLOUD SETUP ---
# Because you ran 'gcloud auth application-default login', this just works!
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

# --- KAFKA SETUP ---
conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': os.getenv('SASL_USERNAME'),
    'sasl.password': os.getenv('SASL_PASSWORD'),
    'group.id': 'python-consumer-gcs-01', # Identifies this specific worker
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC_NAME])

def upload_to_gcs(data_batch):
    """Takes a list of JSON objects and uploads them as a single file to GCS."""
    if not data_batch:
        return
    
    # 1. Create a filename based on current time
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    blob_name = f"raw_telemetry/{timestamp}_batch.json"
    
    # 2. Convert list of dicts to a newline-delimited JSON string (NDJSON)
    # This is the standard format for Big Data (BigQuery loves this)
    file_content = "\n".join([json.dumps(record) for record in data_batch])
    
    # 3. Upload!
    blob = bucket.blob(blob_name)
    blob.upload_from_string(file_content)
    
    print(f"✅ Uploaded {len(data_batch)} records to gs://{BUCKET_NAME}/{blob_name}")

if __name__ == '__main__':
    print(f"🎧 Listening to {TOPIC_NAME} and writing to {BUCKET_NAME}...")
    
    batch_buffer = []

    try:
        while True:
            msg = consumer.poll(1.0) # Wait 1 second for a message

            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            # Add message to our temporary list
            val = json.loads(msg.value().decode('utf-8'))
            batch_buffer.append(val)
            print(f"📥 Buffered: {val['athlete_id']} ({len(batch_buffer)}/{BATCH_SIZE})")

            # If buffer is full, upload to GCS
            if len(batch_buffer) >= BATCH_SIZE:
                upload_to_gcs(batch_buffer)
                batch_buffer = [] # Clear the buffer

    except KeyboardInterrupt:
        print("🛑 Stopping consumer...")
        # Upload whatever is left in the buffer before quitting
        if batch_buffer:
            upload_to_gcs(batch_buffer)
    finally:
        consumer.close()