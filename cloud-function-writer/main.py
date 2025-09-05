import os
import json
import base64
import uuid
import time
from datetime import datetime, timezone

from google.cloud import storage
from bson import json_util
from flask import Flask, request

# --- Globals and Initialization ---
# Get the GCS bucket name from the environment variable set by the deployment script.
GCS_DATA_BUCKET_NAME = os.getenv("GCS_DATA_BUCKET_NAME")

# Initialize the Google Cloud Storage client
storage_client = storage.Client()

# Initialize Flask app for health checks
app = Flask(__name__)

def process_pubsub_message(event, context):
    """
    Cloud Function triggered by a Pub/Sub message.
    It reads the message data, decodes it, and writes the
    JSON payload to a Google Cloud Storage bucket.
    
    OPTIMIZED: Writes JSON directly without unnecessary parse-serialize cycle.

    Args:
        event (dict): The Pub/Sub message payload.
                      This is a dictionary with 'data' and 'attributes' keys.
        context (google.cloud.functions.Context): The Cloud Functions context.
    """
    # Initialize variables for cleanup
    temp_raw_file_path = None
    temp_event_file_path = None
    temp_decoded_file_path = None

    if not GCS_DATA_BUCKET_NAME:
        print("Error: GCS_DATA_BUCKET_NAME environment variable is not set.")
        # Re-raise the exception to trigger the dead-letter queue mechanism
        raise ValueError("GCS_DATA_BUCKET_NAME not set.")

    # Access the GCS bucket object
    try:
        bucket = storage_client.bucket(GCS_DATA_BUCKET_NAME)
    except Exception as e:
        print(f"Error accessing bucket '{GCS_DATA_BUCKET_NAME}': {e}")
        # Re-raise to ensure the message is not acknowledged and retried/DLQ'd
        raise RuntimeError(f"Failed to access GCS bucket: {e}")

    try:
        final_json_str = None
        change_event_payload = None
        
        # The Pub/Sub message data is base64-encoded.
        if 'data' in event:
            # Decode the base64 string to a JSON string
            decoded_initial_data = base64.b64decode(event['data']).decode('utf-8')
            initial_payload = json.loads(decoded_initial_data)

            # Check if the payload is a nested Pub/Sub message from a push subscription
            if 'message' in initial_payload and 'data' in initial_payload['message']:
                # The actual payload is base64-encoded within the 'data' field of the nested 'message' object
                final_json_str = base64.b64decode(initial_payload['message']['data']).decode('utf-8')
                
            # Check if the payload is a direct Pub/Sub message from a pull subscription
            elif 'collection' in initial_payload:
                final_json_str = decoded_initial_data
            else:
                print("Could not find a valid payload structure. Skipping processing.")
                return
        else:
            # Handle cases where the message is empty or malformed
            print("Message data is missing. Skipping processing.")
            return

        # OPTIMIZATION: Parse JSON ONLY to extract metadata for filenames
        # We avoid parsing for the actual data write
        change_event_payload = json_util.loads(final_json_str)

    except (base64.binascii.Error, json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"Error decoding or parsing message data: {e}")
        # We raise an exception to signal that the message was not processed successfully.
        # This will trigger Pub/Sub's retry mechanism or move the message to the DLQ.
        raise ValueError("Failed to decode or parse message data.")

    # Generate a unique filename and GCS path based on the specified structure:
    # `raw/{collection}/{random_prefix}-{year}-{month}-{day}/{operation}_{timestamp}_{unique_id}.json`
    try:
        # Extract metadata for filename (this is why we still need to parse)
        collection = change_event_payload.get("collection", "unknown")
        operation = change_event_payload.get("operation", "unknown")
        
        # Get current UTC time
        now_utc = datetime.now(timezone.utc)
        
        # A simple, short unique prefix for the date folder
        random_prefix = uuid.uuid4().hex[:8]
        
        # Format the date and timestamp for the paths
        date_path = now_utc.strftime("%Y-%m-%d")
        timestamp_str = now_utc.strftime("%Y-%m-%dT%H-%M-%SZ")
        unique_id = uuid.uuid4().hex

        # --- RAW DATA FILE ---
        # Construct the full GCS file path for the raw data
        raw_file_name = (f"raw/{collection}/"
                         f"{random_prefix}-{date_path}/"
                         f"{operation}_{timestamp_str}_{unique_id}.json")
        
        # OPTIMIZATION: Write the JSON string directly without re-serialization
        temp_raw_file_path = f"/tmp/{raw_file_name.replace('/', '_')}"
        with open(temp_raw_file_path, 'w') as temp_file:
            temp_file.write(final_json_str)  # Direct write - no parse/serialize!

        # Upload the raw data temporary file to GCS
        blob_raw = bucket.blob(raw_file_name)
        blob_raw.upload_from_filename(temp_raw_file_path)
        print(f"Successfully wrote raw data for '{collection}' to gs://{GCS_DATA_BUCKET_NAME}/{raw_file_name}")

        # --- DEBUGGING FILES ---
        # For debugging files, we keep pretty-printing since they're for human inspection
        # Construct the full GCS file path for the debugging folder
        debug_folder = "debugging"
        debug_base_path = f"{debug_folder}/{collection}/{random_prefix}-{date_path}/{timestamp_str}_{unique_id}"

        # 1. Write the full event payload to a debug file (pretty-printed for debugging)
        temp_event_file_path = f"/tmp/{debug_base_path.replace('/', '_')}_event.json"
        with open(temp_event_file_path, 'w') as temp_file:
            json.dump(event, temp_file, indent=2)  # Keep pretty-print for debug
        blob_event = bucket.blob(f"{debug_base_path}_event.json")
        blob_event.upload_from_filename(temp_event_file_path)
        print(f"Successfully wrote event payload to gs://{GCS_DATA_BUCKET_NAME}/{blob_event.name}")

        # 2. Write the decoded data to a debug file (pretty-printed for human readability)
        temp_decoded_file_path = f"/tmp/{debug_base_path.replace('/', '_')}_decoded.json"
        with open(temp_decoded_file_path, 'w') as temp_file:
            # For debug file, we pretty-print for human inspection
            temp_file.write(json_util.dumps(change_event_payload, indent=2))
        blob_decoded = bucket.blob(f"{debug_base_path}_decoded.json")
        blob_decoded.upload_from_filename(temp_decoded_file_path)
        print(f"Successfully wrote decoded data to gs://{GCS_DATA_BUCKET_NAME}/{blob_decoded.name}")

    except Exception as e:
        print(f"An unexpected error occurred during GCS upload: {e}")
        # Re-raise the exception to trigger retry/DLQ
        raise RuntimeError("GCS upload failed.")

    finally:
        # Clean up all temporary files safely
        try:
            if temp_raw_file_path and os.path.exists(temp_raw_file_path):
                os.remove(temp_raw_file_path)
            if temp_event_file_path and os.path.exists(temp_event_file_path):
                os.remove(temp_event_file_path)
            if temp_decoded_file_path and os.path.exists(temp_decoded_file_path):
                os.remove(temp_decoded_file_path)
        except OSError as e:
            print(f"Error deleting temporary file: {e}")

# This endpoint is required for the Cloud Run health checks.
# It doesn't do anything but signal that the service is running.
@app.route("/", methods=["POST"])
def health_check():
    return "OK", 200

# This block is for local development. In production (on Cloud Run),
# the gunicorn server defined in the Dockerfile will handle this.
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))