# Fluffy Garbanzo - MongoDB Change Data Capture Pipeline

This project implements a data pipeline on Google Cloud Platform (GCP) to capture change data from a MongoDB database and stream it for downstream processing and storage.

## Overview

The primary goal is to create a reliable and scalable system for ingesting MongoDB change events in real-time. These events are published to a messaging system, processed by a serverless function, and ultimately stored for analysis.

## Architecture

The pipeline follows a serverless, event-driven architecture on GCP:

```
[MongoDB] -> [Cloud Run: mongo-ingestor-v2] -> [Pub/Sub Topic: mongo-change-stream-topic] -> [Cloud Function: mongo-data-writer] -> [GCS]
                                                                  |
                                                                  v
                                                     [Pub/Sub DLQ: mongo-change-events-dlq]
```

### Core Components

*   **`mongo-ingestor-v2` (Cloud Run)**: A service that listens to a MongoDB change stream and publishes each event as a message to Pub/Sub.
*   **Pub/Sub**: Serves as the messaging backbone of the pipeline.
    *   `mongo-change-stream-topic`: The primary topic for all change events.
    *   `mongo-change-events-dlq`: A Dead-Letter Queue (DLQ) to capture messages that fail processing.
*   **`mongo-data-writer` (Cloud Function)**: A function triggered by new messages on the primary topic. It is responsible for processing the event and writing it to a destination (e.g., Google Cloud Storage).

## Current Status

This project is under active development. The following has been completed:

*   **Story 1**: The `mongo-ingestor-v2` service has been developed and deployed.
*   **Story 2**: The core Pub/Sub messaging infrastructure has been configured and validated for reliability, including message retention and a dead-lettering policy.