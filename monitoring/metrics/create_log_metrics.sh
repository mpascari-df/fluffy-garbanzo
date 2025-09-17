#!/bin/bash
# create_log_metrics.sh
# Creates log-based metrics from structured logs for the MongoDB ingestion pipeline
# These metrics will be used in dashboards and alerts

set -e

# Source configuration
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/../../scripts/config_prod.sh"

echo "=========================================="
echo " Creating Log-Based Metrics"
echo "=========================================="
echo "Project: $PROJECT_ID"
echo ""

# Function to create or update a metric
create_metric() {
    local METRIC_NAME=$1
    local DESCRIPTION=$2
    local LOG_FILTER=$3
    
    echo "Creating metric: $METRIC_NAME"
    
    # Check if metric exists
    if gcloud logging metrics describe "$METRIC_NAME" --project="$PROJECT_ID" &>/dev/null; then
        echo "  Metric exists, updating..."
        gcloud logging metrics update "$METRIC_NAME" \
            --description="$DESCRIPTION" \
            --log-filter="$LOG_FILTER" \
            --project="$PROJECT_ID"
    else
        echo "  Creating new metric..."
        gcloud logging metrics create "$METRIC_NAME" \
            --description="$DESCRIPTION" \
            --log-filter="$LOG_FILTER" \
            --project="$PROJECT_ID"
    fi
    echo "  ✓ Done"
    echo ""
}

# ============================================================================
# INGESTION METRICS (from Cloud Run Ingestor)
# ============================================================================

echo "=== Creating Ingestion Metrics ==="

# 1. Change Stream Events
create_metric \
    "change_stream_events" \
    "Count of MongoDB change stream events processed" \
    'resource.labels.service_name="mongo-ingestor-v5" AND textPayload:"METRIC:change_stream_event"'

# 2. Pub/Sub Publish Success
create_metric \
    "pubsub_published" \
    "Successfully published messages to Pub/Sub" \
    'resource.labels.service_name="mongo-ingestor-v5" AND textPayload:"METRIC:pubsub_published"'

# 3. Pub/Sub Publish Failures
create_metric \
    "pubsub_publish_failed" \
    "Failed to publish messages to Pub/Sub" \
    'resource.labels.service_name="mongo-ingestor-v5" AND textPayload:"METRIC:publish_failed"'

# 4. Slow Publishes
create_metric \
    "slow_publishes" \
    "Pub/Sub publishes that took over 1 second" \
    'resource.labels.service_name="mongo-ingestor-v5" AND textPayload:"METRIC:slow_publish"'

# 5. System Stats (periodic health)
create_metric \
    "ingestor_system_stats" \
    "Periodic system statistics from ingestor" \
    'resource.labels.service_name="mongo-ingestor-v5" AND textPayload:"METRIC:system_stats"'

# ============================================================================
# STORAGE METRICS (from Cloud Function)
# ============================================================================

echo "=== Creating Storage Metrics ==="

# 6. Messages Received
create_metric \
    "function_messages_received" \
    "Messages received by Cloud Function" \
    'resource.labels.service_name="mongo-data-writer-v7" AND textPayload:"METRIC:message_received"'

# 7. GCS Write Success
create_metric \
    "gcs_write_success" \
    "Successful writes to GCS" \
    'resource.labels.service_name="mongo-data-writer-v7" AND textPayload:"METRIC:gcs_write_success"'

# 8. GCS Write Failures
create_metric \
    "gcs_write_failed" \
    "Failed writes to GCS" \
    'resource.labels.service_name="mongo-data-writer-v7" AND textPayload:"METRIC:gcs_write_failed"'

# 9. Function Stats
create_metric \
    "function_stats" \
    "Periodic statistics from Cloud Function" \
    'resource.labels.service_name="mongo-data-writer-v7" AND textPayload:"METRIC:function_stats"'

# ============================================================================
# ERROR METRICS (from both services)
# ============================================================================

echo "=== Creating Error Metrics ==="

# 10. Format Errors
create_metric \
    "format_errors" \
    "Data format errors in pipeline" \
    'textPayload:"METRIC:format_error"'

# 11. Unexpected Errors
create_metric \
    "unexpected_errors" \
    "Unexpected errors in pipeline" \
    'textPayload:"METRIC:unexpected_error"'

# ============================================================================
# COLLECTION-SPECIFIC METRICS
# ============================================================================

echo "=== Creating Collection-Specific Metrics ==="

# 12. Events by Collection (using text parsing)
create_metric \
    "events_by_collection" \
    "Events processed grouped by collection name" \
    'textPayload=~"METRIC:change_stream_event|METRIC:gcs_write_success" AND textPayload=~"(customers|leads|changelogs|sysusers|users-metadata)"'

# ============================================================================
# PERFORMANCE METRICS
# ============================================================================

echo "=== Creating Performance Metrics ==="

# 13. High Latency Events (any operation over 1 second)
create_metric \
    "high_latency_operations" \
    "Operations with latency over 1000ms" \
    '(textPayload:"latency" AND textPayload=~"[1-9][0-9]{3,}\\.[0-9]ms")'

# ============================================================================
# DATA LOSS DETECTION
# ============================================================================

echo "=== Creating Data Loss Detection Metrics ==="

# 14. Critical: Data Loss Events
create_metric \
    "data_loss_events" \
    "CRITICAL: Potential data loss detected" \
    '(textPayload:"DATA LOSS" OR textPayload:"Message dropped" OR textPayload:"Failed to send to DLQ")'

# ============================================================================
# SUMMARY
# ============================================================================

echo "=========================================="
echo " Metrics Creation Complete!"
echo "=========================================="
echo ""
echo "Created 14 log-based metrics:"
echo "  ✓ Ingestion: 5 metrics"
echo "  ✓ Storage: 4 metrics"
echo "  ✓ Errors: 2 metrics"
echo "  ✓ Collections: 1 metric"
echo "  ✓ Performance: 1 metric"
echo "  ✓ Data Loss: 1 metric"
echo ""
echo "These metrics will start collecting data immediately."
echo "Data will be available in dashboards after ~2 minutes."
echo ""
echo "To view metrics:"
echo "  gcloud logging metrics list --project=$PROJECT_ID"
echo ""
echo "To test a metric:"
echo "  gcloud logging read 'textPayload:\"METRIC:change_stream_event\"' \\"
echo "    --project=$PROJECT_ID --limit=5"
echo ""