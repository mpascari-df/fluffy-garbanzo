#!/bin/bash
# verify_monitoring.sh
# Verifies that monitoring components are working correctly

set -e

# Source configuration
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/../../scripts/config_prod.sh"

echo "=========================================="
echo " Verifying Monitoring Setup"
echo "=========================================="
echo "Project: $PROJECT_ID"
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Tracking
TOTAL_CHECKS=0
PASSED_CHECKS=0

# Function to check a condition
check() {
    local description=$1
    local command=$2
    
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    echo -n "Checking: $description... "
    
    if eval "$command" &>/dev/null; then
        echo -e "${GREEN}✓${NC}"
        PASSED_CHECKS=$((PASSED_CHECKS + 1))
        return 0
    else
        echo -e "${RED}✗${NC}"
        return 1
    fi
}

# Function to check metric exists
check_metric() {
    local metric_name=$1
    check "$metric_name metric exists" \
        "gcloud logging metrics describe '$metric_name' --project='$PROJECT_ID'"
}

echo "=== 1. Checking Services ==="
check "Cloud Run service is running" \
    "gcloud run services describe mongo-ingestor-v5 --region=$REGION --project=$PROJECT_ID"

check "Cloud Function is active" \
    "gcloud functions describe mongo-data-writer-v7 --region=$REGION --project=$PROJECT_ID"

echo ""
echo "=== 2. Checking Log Metrics ==="
check_metric "change_stream_events"
check_metric "pubsub_published"
check_metric "gcs_write_success"
check_metric "data_loss_events"

echo ""
echo "=== 3. Checking Recent Logs ==="
check "Recent METRIC logs exist" \
    "gcloud logging read 'textPayload:\"METRIC:\" AND timestamp>=\"'\"$(date -u -d '1 hour ago' '+%Y-%m-%dT%H:%M:%S')\"'\"' --project=$PROJECT_ID --limit=1"

check "Ingestor generating logs" \
    "gcloud logging read 'resource.labels.service_name=\"mongo-ingestor-v5\" AND timestamp>=\"'\"$(date -u -d '1 hour ago' '+%Y-%m-%dT%H:%M:%S')\"'\"' --project=$PROJECT_ID --limit=1"

check "Cloud Function generating logs" \
    "gcloud logging read 'resource.labels.service_name=\"mongo-data-writer-v7\" AND timestamp>=\"'\"$(date -u -d '1 hour ago' '+%Y-%m-%dT%H:%M:%S')\"'\"' --project=$PROJECT_ID --limit=1"

echo ""
echo "=== 4. Checking Data Flow ==="

# Get recent counts
INGESTED=$(gcloud logging read 'textPayload:"METRIC:change_stream_event" AND timestamp>="'$(date -u -d '10 minutes ago' '+%Y-%m-%dT%H:%M:%S')'"' \
    --project=$PROJECT_ID --format="value(textPayload)" 2>/dev/null | wc -l)

PUBLISHED=$(gcloud logging read 'textPayload:"METRIC:pubsub_published" AND timestamp>="'$(date -u -d '10 minutes ago' '+%Y-%m-%dT%H:%M:%S')'"' \
    --project=$PROJECT_ID --format="value(textPayload)" 2>/dev/null | wc -l)

STORED=$(gcloud logging read 'textPayload:"METRIC:gcs_write_success" AND timestamp>="'$(date -u -d '10 minutes ago' '+%Y-%m-%dT%H:%M:%S')'"' \
    --project=$PROJECT_ID --format="value(textPayload)" 2>/dev/null | wc -l)

echo "Last 10 minutes activity:"
echo "  • Events ingested:    $INGESTED"
echo "  • Events published:   $PUBLISHED"
echo "  • Events stored:      $STORED"

if [ "$INGESTED" -gt 0 ]; then
    echo -e "  ${GREEN}✓ Data is flowing${NC}"
else
    echo -e "  ${YELLOW}⚠ No recent activity detected${NC}"
fi

echo ""
echo "=== 5. Checking for Errors ==="

ERROR_COUNT=$(gcloud logging read 'severity>=ERROR AND timestamp>="'$(date -u -d '1 hour ago' '+%Y-%m-%dT%H:%M:%S')'"' \
    --project=$PROJECT_ID --format="value(textPayload)" 2>/dev/null | wc -l)

if [ "$ERROR_COUNT" -eq 0 ]; then
    echo -e "  ${GREEN}✓ No errors in last hour${NC}"
else
    echo -e "  ${YELLOW}⚠ Found $ERROR_COUNT errors in last hour${NC}"
    echo "  To view: gcloud logging read 'severity>=ERROR' --project=$PROJECT_ID --limit=5"
fi

# Check for data loss
DATA_LOSS=$(gcloud logging read 'textPayload:"DATA LOSS" AND timestamp>="'$(date -u -d '24 hours ago' '+%Y-%m-%dT%H:%M:%S')'"' \
    --project=$PROJECT_ID --format="value(textPayload)" 2>/dev/null | wc -l)

if [ "$DATA_LOSS" -eq 0 ]; then
    echo -e "  ${GREEN}✓ No data loss detected${NC}"
else
    echo -e "  ${RED}✗ WARNING: $DATA_LOSS data loss events detected!${NC}"
fi

echo ""
echo "=========================================="
echo " Verification Summary"
echo "=========================================="
echo "Checks passed: $PASSED_CHECKS / $TOTAL_CHECKS"

if [ "$PASSED_CHECKS" -eq "$TOTAL_CHECKS" ]; then
    echo -e "${GREEN}✓ All checks passed!${NC}"
    exit 0
elif [ "$PASSED_CHECKS" -ge $((TOTAL_CHECKS * 3 / 4)) ]; then
    echo -e "${YELLOW}⚠ Most checks passed, review warnings above${NC}"
    exit 0
else
    echo -e "${RED}✗ Several checks failed, review issues above${NC}"
    exit 1
fi