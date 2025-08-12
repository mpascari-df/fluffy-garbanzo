# main.py
import os
import json
import signal
import threading
import time
import uuid
from datetime import datetime, timezone

import pymongo
from bson import json_util
from google.cloud import pubsub_v1
from google.cloud import secretmanager
from flask import Flask, request

# --- Configuration ---
PROJECT_ID = os.getenv("PROJECT_ID")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
PUBSUB_TOPIC_NAME = os.getenv("PUBSUB_TOPIC_NAME")
PUBSUB_DEAD_LETTER_TOPIC_NAME = os.getenv("PUBSUB_DEAD_LETTER_TOPIC")
MONGO_URI_SECRET_ID = "mongo-uri" # The name of the secret we created

# --- Helper function to access Secret Manager ---
def access_secret_version(project_id, secret_id, version_id="latest"):
    """
    Access the payload for the given secret version and return it.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# --- Initialize clients ---
MONGO_URI = access_secret_version(PROJECT_ID, MONGO_URI_SECRET_ID)
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
        "document": document,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "oplog_timestamp": str(change.get("clusterTime")),
        "correlation_id": str(uuid.uuid4()),
    }
    return message

def publish_to_pubsub(message_data):
    """
    Publishes a message to the main Pub/Sub topic.
    If publishing fails after retries, it attempts to publish to a dead-letter topic.
    """
    try:
        # Use json_util.dumps for robust serialization of BSON types like ObjectId
        payload = json_util.dumps(message_data).encode("utf-8")

        # The publisher client has built-in retry logic for transient errors.
        # We are relying on the default robust policies which use exponential backoff.
        # See: https://cloud.google.com/pubsub/docs/publisher#using_retry_policies
        future = publisher.publish(topic_path, payload)

        # .result() blocks until the message is published or a non-retryable
        # error occurs. A timeout is added to prevent the thread from blocking
        # indefinitely, which is critical for graceful shutdown.
        message_id = future.result(timeout=5.0)
        print(f"Published message ID: {message_id} for operation: {message_data['operation']} on collection: {message_data['collection']}")
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
                # If even the DLQ fails, we have a serious problem.
                # Log it loudly. The message is now lost.
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
    # In a real-world scenario, this token should be persisted to a database or
    # a file to survive service restarts. For now, it handles reconnections.
    resume_token = None

    while not shutdown_event.is_set():
        try:
            pipeline = [{'$match': {'operationType': {'$in': ['insert', 'update', 'delete']}}}]
            # Start watching with a resume token if we have one from a previous iteration
            with db.watch(pipeline, full_document='updateLookup', resume_after=resume_token) as stream:
                if resume_token is None:
                    print("Successfully connected to MongoDB change stream.")
                else:
                    print(f"Resumed MongoDB change stream with token.")

                resume_token = stream.resume_token

                # Use stream.try_next() in a loop for non-blocking iteration.
                # This allows us to check for the shutdown signal periodically.
                while stream.alive and not shutdown_event.is_set():
                    change = stream.try_next()
                    if change is None:
                        # No new change, wait a bit to prevent a busy loop.
                        time.sleep(0.5)
                        continue

                    print(f"Received change event: {change['_id']}")
                    formatted_message = format_change_event(change)
                    if formatted_message:
                        publish_to_pubsub(formatted_message)

                    # Always update the resume token after processing a change
                    resume_token = stream.resume_token

        except pymongo.errors.PyMongoError as e:
            print(f"ERROR: MongoDB connection error: {e}. Reconnecting in 5 seconds...")
            # Don't try to reconnect if we are shutting down.
            if not shutdown_event.is_set():
                time.sleep(5)
        except Exception as e:
            print(f"ERROR: An unexpected error occurred in listener: {e}. Restarting listener in 5 seconds...")
            if not shutdown_event.is_set():
                time.sleep(5)

    print("Shutdown signal received. Change stream listener has stopped.")

# --- Background Thread & Health Check ---

# Start the listener in a background thread. This is crucial for Cloud Run,
# as it allows the main thread to serve HTTP requests (like health checks)
# while the listener runs continuously.
listener_thread = threading.Thread(target=start_change_stream_listener)

def signal_handler(signum, frame):
    """
    Gracefully handle termination signals (like SIGTERM from Cloud Run).
    This function signals the listener thread to stop, waits for it to finish,
    and then cleans up resources like the MongoDB client connection.
    """
    print(f"Received signal {signum}. Initiating graceful shutdown...")

    # 1. Signal the listener thread to stop its work.
    shutdown_event.set()

    # 2. Wait for the listener thread to finish. Cloud Run gives a 10-second
    # grace period before sending SIGKILL. We wait for a slightly shorter
    # time to allow for final cleanup steps.
    listener_thread.join(timeout=8.0)

    # 3. Clean up the MongoDB client connection pool.
    print("Closing MongoDB client connection...")
    mongo_client.close()
    print("Shutdown complete.")

signal.signal(signal.SIGTERM, signal_handler)
listener_thread.daemon = True
listener_thread.start()

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

# This block is for local development. Gunicorn is used in production via the Dockerfile.
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
