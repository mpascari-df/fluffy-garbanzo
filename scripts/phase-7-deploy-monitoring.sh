#!/bin/bash
# phase-7-deploy-monitoring.sh
# Deploys monitoring infrastructure for the MongoDB ingestion pipeline

set -e

# Source configuration
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/config_prod.sh"

echo "=========================================="
echo " Phase 7: Monitoring Deployment"
echo "=========================================="
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo ""
echo "This will deploy:"
echo "  • Log-based metrics"
echo "  • Monitoring dashboards"
echo "  • Alert policies"
echo ""

# Check prerequisites
echo "=== Checking Prerequisites ==="

# Check if services are deployed
echo -n "Checking Cloud Run service... "
if gcloud run services describe "$CLOUD_RUN_SERVICE_NAME" \
    --region="$REGION" \
    --project="$PROJECT_ID" &>/dev/null; then
    echo "✓"
else
    echo "✗"
    echo "ERROR: Cloud Run service not found. Deploy it first with phase-3."
    exit 1
fi

echo -n "Checking Cloud Function... "
if gcloud functions describe "$CLOUD_FUNCTION_NAME" \
    --region="$REGION" \
    --project="$PROJECT_ID" &>/dev/null; then
    echo "✓"
else
    echo "✗"
    echo "ERROR: Cloud Function not found. Deploy it first with phase-4."
    exit 1
fi

# Check if monitoring directory exists
MONITORING_DIR="${SCRIPT_DIR}/../monitoring"
if [ ! -d "$MONITORING_DIR" ]; then
    echo ""
    echo "ERROR: Monitoring directory not found at $MONITORING_DIR"
    echo "Please ensure the monitoring/ directory exists with setup scripts."
    exit 1
fi

echo ""
echo "=== Deploying Monitoring Infrastructure ==="

# Run the master setup script
if [ -f "${MONITORING_DIR}/setup_all.sh" ]; then
    bash "${MONITORING_DIR}/setup_all.sh"
else
    # Fallback to individual scripts
    echo "Running individual setup scripts..."
    
    # Create metrics
    if [ -f "${MONITORING_DIR}/metrics/create_log_metrics.sh" ]; then
        echo ""
        echo "Creating log-based metrics..."
        bash "${MONITORING_DIR}/metrics/create_log_metrics.sh"
    fi
    
    # Import dashboards (when available)
    if [ -f "${MONITORING_DIR}/dashboards/import_dashboards.sh" ]; then
        echo ""
        echo "Importing dashboards..."
        bash "${MONITORING_DIR}/dashboards/import_dashboards.sh"
    fi
    
    # Create alerts (when available)
    if [ -f "${MONITORING_DIR}/alerts/create_alerts.sh" ]; then
        echo ""
        echo "Creating alert policies..."
        bash "${MONITORING_DIR}/alerts/create_alerts.sh"
    fi
fi

# Verify deployment
echo ""
echo "=== Verifying Deployment ==="

if [ -f "${MONITORING_DIR}/utils/verify_monitoring.sh" ]; then
    bash "${MONITORING_DIR}/utils/verify_monitoring.sh"
else
    # Simple verification
    echo "Checking metrics..."
    METRIC_COUNT=$(gcloud logging metrics list --project="$PROJECT_ID" \
        --format="value(name)" | \
        grep -E "^(change_stream_events|pubsub_published|gcs_write_success)" | \
        wc -l)
    
    if [ "$METRIC_COUNT" -gt 0 ]; then
        echo "✓ Found $METRIC_COUNT monitoring metrics"
    else
        echo "⚠ Warning: No metrics found"
    fi
fi

echo ""
echo "=========================================="
echo " Phase 7 Complete!"
echo "=========================================="
echo ""
echo "✅ Monitoring infrastructure deployed"
echo ""
echo "View your metrics:"
echo "  https://console.cloud.google.com/logs/metrics?project=$PROJECT_ID"
echo ""
echo "View logs:"
echo "  https://console.cloud.google.com/logs/query?project=$PROJECT_ID"
echo ""
echo "To see metric data (wait 2-3 minutes):"
echo "  1. Go to Metrics Explorer:"
echo "     https://console.cloud.google.com/monitoring/metrics-explorer?project=$PROJECT_ID"
echo "  2. Select 'Resource Type: Logging' "
echo "  3. Choose metrics like:"
echo "     - logging.googleapis.com/user/change_stream_events"
echo "     - logging.googleapis.com/user/gcs_write_success"
echo ""
echo "Next steps:"
echo "  1. Create custom dashboards in Cloud Monitoring"
echo "  2. Set up alert notification channels"
echo "  3. Configure SLOs for critical paths"
echo ""