#!/bin/bash
# delete_log_metrics.sh
# Removes log-based metrics if needed for cleanup

set -e

# Source configuration
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/../../scripts/config_prod.sh"

echo "=========================================="
echo " Deleting Log-Based Metrics"
echo "=========================================="
echo "Project: $PROJECT_ID"
echo ""

# List of metrics to delete
METRICS=(
    "change_stream_events"
    "pubsub_published"
    "pubsub_publish_failed"
    "slow_publishes"
    "ingestor_system_stats"
    "function_messages_received"
    "gcs_write_success"
    "gcs_write_failed"
    "function_stats"
    "format_errors"
    "unexpected_errors"
    "events_by_collection"
    "high_latency_operations"
    "data_loss_events"
)

# Confirmation
echo "This will delete ${#METRICS[@]} metrics."
read -p "Are you sure? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cancelled."
    exit 0
fi

# Delete each metric
for metric in "${METRICS[@]}"; do
    echo -n "Deleting $metric... "
    if gcloud logging metrics delete "$metric" --project="$PROJECT_ID" --quiet 2>/dev/null; then
        echo "✓"
    else
        echo "not found or already deleted"
    fi
done

echo ""
echo "Cleanup complete!"