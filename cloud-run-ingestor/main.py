# main.py for Cloud Run Service
# This service listens to a MongoDB change stream and publishes
# events to a Pub/Sub topic in CloudEvents format.
#
# This modified version also includes a new POST endpoint for manual testing.

import os
import json
import signal
import threading
import time
import uuid
import base64
from datetime import datetime, timezone

import pymongo
from bson import json_util
from google.cloud import pubsub_v1
from flask import Flask, request

# --- Configuration ---
PROJECT_ID = os.getenv("PROJECT_ID")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
PUBSUB_TOPIC_NAME = os.getenv("PUBSUB_TOPIC_NAME")
PUBSUB_DEAD_LETTER_TOPIC_NAME = os.getenv("PUBLISHER_DLQ_TOPIC_NAME")
# The MONGO_URI is now injected directly by Cloud Run as an environment variable.
MONGO_URI = os.getenv("MONGO_URI")

# --- Initialize clients ---
mongo_client = pymongo.MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB_NAME]

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC_NAME)
dead_letter_topic_path = None
if PUBSUB_DEAD_LETTER_TOPIC_NAME:
    dead_letter_topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_DEAD_LETTER_TOPIC_NAME)

app = Flask(__name__)

# --- Core Logic ---
def format_change_event(change):
    """
    Formats a MongoDB change event into the standardized JSON structure.
    """
    operation = change.get("operationType")
    if not operation:
        return None

    document = None
    if operation in ["insert", "update"]:
        document = change.get("fullDocument")
    elif operation == "delete":
        # For deletes, the full document is not available. We capture the key.
        document = change.get("documentKey")

    if document is None:
        return None

    # Create the standardized message payload
    message = {
        "operation": operation,
        "collection": change.get("ns", {}).get("coll"),
        "database": change.get("ns", {}).get("db"), # Added for consistency
        "document": document,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "oplog_timestamp": str(change.get("clusterTime")),
        "correlation_id": str(uuid.uuid4()),
    }
    return message

def publish_to_pubsub(message_data):
    """
    Publishes a message to the main Pub/Sub topic in CloudEvents format.
    If publishing fails after retries, it attempts to publish to a dead-letter topic.
    """
    try:
        # Use json_util.dumps for robust serialization of BSON types like ObjectId
        # First, serialize the message data to JSON string
        json_payload = json_util.dumps(message_data)
        
        # Second, encode it to base64 as required for a CloudEvent
        encoded_data = base64.b64encode(json_payload.encode('utf-8')).decode('utf-8')

        # Third, wrap the encoded data in the CloudEvents structure
        event_payload = {
            "message": {
                "data": encoded_data,
                "attributes": {
                    "operation": message_data.get("operation", "unknown"),
                    "collection": message_data.get("collection", "unknown")
                }
            }
        }
        
        # Finally, publish the CloudEvent-formatted payload
        # The Pub/Sub client handles the final encoding to bytes for the API call.
        future = publisher.publish(topic_path, json.dumps(event_payload).encode("utf-8"))

        message_id = future.result(timeout=5.0)
        print(f"Published message ID: {message_id} for operation: {message_data.get('operation')} on collection: {message_data.get('collection')}")
    except Exception as e:
        print(f"ERROR: Failed to publish message to Pub/Sub topic '{PUBSUB_TOPIC_NAME}': {e}")
        # If publishing fails, try sending to the dead-letter topic if configured.
        if dead_letter_topic_path:
            print(f"Attempting to publish to dead-letter topic '{PUBSUB_DEAD_LETTER_TOPIC_NAME}'...")
            try:
                # Add error context to the message before sending to DLQ
                message_data["_error_context"] = {
                    "original_topic": PUBSUB_TOPIC_NAME,
                    "error_message": str(e),
                    "error_timestamp": datetime.now(timezone.utc).isoformat()
                }
                payload = json_util.dumps(message_data).encode("utf-8")
                future = publisher.publish(dead_letter_topic_path, payload)
                dlq_message_id = future.result(timeout=5.0)
                print(f"Successfully published message to dead-letter topic. DLQ Message ID: {dlq_message_id}")
            except Exception as dlq_e:
                print(f"CRITICAL: Failed to publish message to dead-letter topic '{PUBSUB_DEAD_LETTER_TOPIC_NAME}'. DATA LOSS OCCURRED. Error: {dlq_e}")
                print(f"Lost message payload: {message_data}")
        else:
            print("CRITICAL: No dead-letter topic configured. DATA LOSS OCCURRED.")
            print(f"Lost message payload: {message_data}")

shutdown_event = threading.Event()

def start_change_stream_listener():
    """
    Establishes a resilient MongoDB change stream listener that automatically reconnects
    and handles graceful shutdown.
    """
    print(f"Starting change stream listener for database '{MONGO_DB_NAME}'...")
    resume_token = None

    while not shutdown_event.is_set():
        try:
            pipeline = [{'$match': {'operationType': {'$in': ['insert', 'update', 'delete']}}}]
            with db.watch(pipeline, full_document='updateLookup', resume_after=resume_token) as stream:
                if resume_token is None:
                    print("Successfully connected to MongoDB change stream.")
                else:
                    print(f"Resumed MongoDB change stream with token.")

                resume_token = stream.resume_token

                while stream.alive and not shutdown_event.is_set():
                    change = stream.try_next()
                    if change is None:
                        time.sleep(0.5)
                        continue

                    print(f"Received change event: {change['_id']}")
                    formatted_message = format_change_event(change)
                    if formatted_message:
                        publish_to_pubsub(formatted_message)

                    resume_token = stream.resume_token

        except pymongo.errors.PyMongoError as e:
            print(f"ERROR: MongoDB connection error: {e}. Reconnecting in 5 seconds...")
            if not shutdown_event.is_set():
                time.sleep(5)
        except Exception as e:
            print(f"ERROR: An unexpected error occurred in listener: {e}. Restarting listener in 5 seconds...")
            if not shutdown_event.is_set():
                time.sleep(5)

    print("Shutdown signal received. Change stream listener has stopped.")

# --- Background Thread & Health Check ---
listener_thread = threading.Thread(target=start_change_stream_listener)

def signal_handler(signum, frame):
    """
    Gracefully handle termination signals (like SIGTERM from Cloud Run).
    """
    print(f"Received signal {signum}. Initiating graceful shutdown...")

    shutdown_event.set()

    listener_thread.join(timeout=8.0)

    print("Closing MongoDB client connection...")
    mongo_client.close()
    print("Shutdown complete.")

signal.signal(signal.SIGTERM, signal_handler)
listener_thread.daemon = True
listener_thread.start()

# --- Health Check Endpoints ---
@app.route('/health')
def health_check():
    """
    Health check endpoint for Cloud Run.
    Checks if the listener thread is alive.
    """
    if listener_thread.is_alive():
        return 'Service is running and listener thread is active.', 200
    else:
        return 'Error: Listener thread has stopped.', 500

# A simple root endpoint
@app.route('/')
def index():
    return 'MongoDB Change Stream Ingestor. Use /health for status.', 200

# --- New Route for Manual Testing ---
@app.route('/test-publish', methods=['POST'])
def test_manual_publish():
    """
    Endpoint for manually testing the Pub/Sub publishing logic.
    Accepts a JSON payload and publishes it to Pub/Sub.
    """
    try:
        data = request.get_json(silent=True)
        if not data:
            return "Invalid JSON payload or no data provided.", 400

        # The data is in the format expected by publish_to_pubsub
        publish_to_pubsub(data)
        
        return "Message payload received and sent to Pub/Sub successfully!", 200

    except Exception as e:
        print(f"ERROR: Failed to process manual test publish request: {e}")
        return "An internal server error occurred during processing.", 500

# This block is for local development. Gunicorn is used in production via the Dockerfile.
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
