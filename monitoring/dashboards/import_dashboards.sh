#!/bin/bash
# import_dashboards.sh
# Imports monitoring dashboards to Cloud Monitoring

set -e

# Source configuration
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/../../scripts/config_prod.sh"

echo "=========================================="
echo " Importing Dashboards"
echo "=========================================="
echo "Project: $PROJECT_ID"
echo ""

# Function to import a dashboard
import_dashboard() {
    local DASHBOARD_FILE=$1
    local DASHBOARD_NAME=$(basename "$DASHBOARD_FILE" .json)
    
    if [ ! -f "$DASHBOARD_FILE" ]; then
        echo "⚠ Dashboard file not found: $DASHBOARD_FILE"
        return 1
    fi
    
    echo -n "Importing $DASHBOARD_NAME... "
    
    # Create dashboard using gcloud
    if gcloud monitoring dashboards create --config-from-file="$DASHBOARD_FILE" \
        --project="$PROJECT_ID" 2>/dev/null; then
        echo "✓"
        return 0
    else
        # Try updating if it exists
        # Extract dashboard ID from the file (if it has one)
        local DASHBOARD_ID=$(grep -o '"name":[[:space:]]*"[^"]*"' "$DASHBOARD_FILE" 2>/dev/null | cut -d'"' -f4 | cut -d'/' -f4)
        
        if [ -z "$DASHBOARD_ID" ]; then
            # Generate ID from display name
            DASHBOARD_ID=$(grep '"displayName"' "$DASHBOARD_FILE" | cut -d'"' -f4 | tr ' ' '-' | tr '[:upper:]' '[:lower:]')
        fi
        
        if [ -n "$DASHBOARD_ID" ] && gcloud monitoring dashboards update "$DASHBOARD_ID" \
            --config-from-file="$DASHBOARD_FILE" \
            --project="$PROJECT_ID" 2>/dev/null; then
            echo "✓ (updated)"
            return 0
        else
            echo "✗ (may already exist with different ID)"
            return 1
        fi
    fi
}

# Import each dashboard
DASHBOARDS_DIR="$SCRIPT_DIR"
SUCCESS=0
FAILED=0

for dashboard in "$DASHBOARDS_DIR"/dashboard_*.json; do
    if [ -f "$dashboard" ]; then
        if import_dashboard "$dashboard"; then
            SUCCESS=$((SUCCESS + 1))
        else
            FAILED=$((FAILED + 1))
        fi
    fi
done

echo ""
echo "=========================================="
echo " Import Complete"
echo "=========================================="
echo "Imported: $SUCCESS dashboards"
if [ "$FAILED" -gt 0 ]; then
    echo "Failed: $FAILED dashboards"
fi
echo ""
echo "View dashboards at:"
echo "  https://console.cloud.google.com/monitoring/dashboards?project=$PROJECT_ID"
echo ""

# List imported dashboards
echo "Available dashboards:"
gcloud monitoring dashboards list --project="$PROJECT_ID" --format="table(displayName, name)" 2>/dev/null || true