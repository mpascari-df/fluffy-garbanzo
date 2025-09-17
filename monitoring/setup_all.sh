#!/bin/bash
# setup_all.sh
# Master script to set up all monitoring components

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo "=========================================="
echo " MongoDB Pipeline Monitoring Setup"
echo "=========================================="
echo ""
echo "This will set up:"
echo "  1. Log-based metrics"
echo "  2. Monitoring dashboards"
echo "  3. Alert policies"
echo ""
read -p "Continue? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Setup cancelled."
    exit 0
fi

# Step 1: Create log metrics
echo ""
echo "=== Step 1: Creating Log Metrics ==="
bash "${SCRIPT_DIR}/metrics/create_log_metrics.sh"

# Step 2: Import dashboards (if they exist)
if [ -f "${SCRIPT_DIR}/dashboards/import_dashboards.sh" ]; then
    echo ""
    echo "=== Step 2: Importing Dashboards ==="
    bash "${SCRIPT_DIR}/dashboards/import_dashboards.sh"
else
    echo ""
    echo "=== Step 2: Dashboards ==="
    echo "  Dashboards not yet configured. Skipping..."
fi

# Step 3: Create alerts (if they exist)
if [ -f "${SCRIPT_DIR}/alerts/create_alerts.sh" ]; then
    echo ""
    echo "=== Step 3: Creating Alert Policies ==="
    bash "${SCRIPT_DIR}/alerts/create_alerts.sh"
else
    echo ""
    echo "=== Step 3: Alerts ==="
    echo "  Alerts not yet configured. Skipping..."
fi

# Step 4: Verify
if [ -f "${SCRIPT_DIR}/utils/verify_monitoring.sh" ]; then
    echo ""
    echo "=== Step 4: Verifying Setup ==="
    bash "${SCRIPT_DIR}/utils/verify_monitoring.sh"
else
    echo ""
    echo "=== Step 4: Verification ==="
    # Simple verification
    echo "Checking created metrics..."
    source "${SCRIPT_DIR}/../scripts/config_prod.sh"
    
    METRIC_COUNT=$(gcloud logging metrics list --project="$PROJECT_ID" --format="value(name)" | grep -E "^(change_stream_events|pubsub_published|gcs_write_success)" | wc -l)
    echo "  Found $METRIC_COUNT monitoring metrics"
    
    if [ "$METRIC_COUNT" -gt 0 ]; then
        echo "  ✓ Metrics created successfully"
    else
        echo "  ⚠ Warning: Expected metrics not found"
    fi
fi

echo ""
echo "=========================================="
echo " Monitoring Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Wait 2-3 minutes for metrics to populate"
echo "  2. View metrics in Cloud Console:"
echo "     https://console.cloud.google.com/logs/metrics?project=${PROJECT_ID}"
echo "  3. Create custom dashboards in Cloud Monitoring"
echo ""
echo "To test metrics are working:"
echo "  gcloud logging read 'textPayload:\"METRIC:\"' \\"
echo "    --project=${PROJECT_ID} --limit=10"
echo ""