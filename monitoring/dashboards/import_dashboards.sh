#!/bin/bash
# import_dashboards.sh
# Smart import script that handles both creating new and updating existing dashboards
# Checks by display name to avoid ID mismatch issues

set -e

# Source configuration
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CONFIG_FILE="${SCRIPT_DIR}/../../scripts/config_prod.sh"

# Check for config file
if [ ! -f "$CONFIG_FILE" ]; then
    # Try alternate location
    CONFIG_FILE="${SCRIPT_DIR}/../../scripts/config.sh"
fi

if [ ! -f "$CONFIG_FILE" ]; then
    echo "ERROR: Configuration file not found in expected locations"
    echo "Tried: ${SCRIPT_DIR}/../../scripts/config_prod.sh"
    echo "Tried: ${SCRIPT_DIR}/../../scripts/config.sh"
    exit 1
fi

source "$CONFIG_FILE"

echo "=========================================="
echo " Smart Dashboard Import/Update"
echo "=========================================="
echo "Project: $PROJECT_ID"
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to import or update a dashboard
import_or_update_dashboard() {
    local DASHBOARD_FILE=$1
    local DASHBOARD_FILENAME=$(basename "$DASHBOARD_FILE")
    
    if [ ! -f "$DASHBOARD_FILE" ]; then
        echo -e "${RED}⚠${NC} Dashboard file not found: $DASHBOARD_FILE"
        return 1
    fi
    
    # Extract displayName from the JSON file
    local DISPLAY_NAME=$(grep -o '"displayName"[[:space:]]*:[[:space:]]*"[^"]*"' "$DASHBOARD_FILE" | head -1 | sed 's/.*:[[:space:]]*"\(.*\)"/\1/')
    
    if [ -z "$DISPLAY_NAME" ]; then
        echo -e "${RED}⚠${NC} Could not extract display name from $DASHBOARD_FILENAME"
        return 1
    fi
    
    echo -n "Processing: $DISPLAY_NAME "
    echo -n "($DASHBOARD_FILENAME)... "
    
    # Check if a dashboard with this display name already exists
    local EXISTING_ID=$(gcloud monitoring dashboards list \
        --project="$PROJECT_ID" \
        --filter="displayName=\"$DISPLAY_NAME\"" \
        --format="value(name)" 2>/dev/null | head -n1)
    
    if [ -n "$EXISTING_ID" ]; then
        # Dashboard exists, update it
        local DASHBOARD_ID=$(basename "$EXISTING_ID")
        echo -n "found existing, updating... "
        
        # Create a temporary file with the config
        local TEMP_FILE=$(mktemp)
        cp "$DASHBOARD_FILE" "$TEMP_FILE"
        
        # Try to update the existing dashboard
        if gcloud monitoring dashboards update "$DASHBOARD_ID" \
            --config-from-file="$TEMP_FILE" \
            --project="$PROJECT_ID" 2>/dev/null; then
            echo -e "${GREEN}✓ updated${NC}"
            rm -f "$TEMP_FILE"
            return 0
        else
            # If update fails, try delete and recreate
            echo -n "update failed, recreating... "
            
            # Delete the existing dashboard
            if gcloud monitoring dashboards delete "$DASHBOARD_ID" \
                --project="$PROJECT_ID" --quiet 2>/dev/null; then
                
                # Create new dashboard
                if gcloud monitoring dashboards create \
                    --config-from-file="$TEMP_FILE" \
                    --project="$PROJECT_ID" 2>/dev/null; then
                    echo -e "${GREEN}✓ recreated${NC}"
                    rm -f "$TEMP_FILE"
                    return 0
                else
                    echo -e "${RED}✗ recreation failed${NC}"
                    rm -f "$TEMP_FILE"
                    return 1
                fi
            else
                echo -e "${RED}✗ could not delete for recreation${NC}"
                rm -f "$TEMP_FILE"
                return 1
            fi
        fi
    else
        # Dashboard doesn't exist, create it
        echo -n "not found, creating new... "
        
        if gcloud monitoring dashboards create \
            --config-from-file="$DASHBOARD_FILE" \
            --project="$PROJECT_ID" 2>/dev/null; then
            echo -e "${GREEN}✓ created${NC}"
            return 0
        else
            # Check if it might have been created with a slightly different name
            local PARTIAL_NAME=$(echo "$DISPLAY_NAME" | cut -d' ' -f1-3)
            local SIMILAR_EXISTS=$(gcloud monitoring dashboards list \
                --project="$PROJECT_ID" \
                --filter="displayName:\"$PARTIAL_NAME\"" \
                --format="value(name)" 2>/dev/null | head -n1)
            
            if [ -n "$SIMILAR_EXISTS" ]; then
                echo -e "${YELLOW}⚠ similar dashboard may already exist${NC}"
                return 1
            else
                echo -e "${RED}✗ creation failed${NC}"
                return 1
            fi
        fi
    fi
}

# Main execution
echo "Checking for dashboard files..."
DASHBOARDS_DIR="$SCRIPT_DIR"
TOTAL_FILES=0
for dashboard in "$DASHBOARDS_DIR"/dashboard_*.json; do
    if [ -f "$dashboard" ]; then
        TOTAL_FILES=$((TOTAL_FILES + 1))
    fi
done

if [ "$TOTAL_FILES" -eq 0 ]; then
    echo -e "${RED}ERROR: No dashboard JSON files found in $DASHBOARDS_DIR${NC}"
    exit 1
fi

echo "Found $TOTAL_FILES dashboard files to process"
echo ""

# Import each dashboard
SUCCESS=0
FAILED=0
FAILED_DASHBOARDS=""

for dashboard in "$DASHBOARDS_DIR"/dashboard_*.json; do
    if [ -f "$dashboard" ]; then
        if import_or_update_dashboard "$dashboard"; then
            SUCCESS=$((SUCCESS + 1))
        else
            FAILED=$((FAILED + 1))
            FAILED_DASHBOARDS="$FAILED_DASHBOARDS\n  - $(basename $dashboard)"
        fi
    fi
done

echo ""
echo "=========================================="
echo " Import Summary"
echo "=========================================="
echo -e "Success: ${GREEN}$SUCCESS${NC} dashboards"
if [ "$FAILED" -gt 0 ]; then
    echo -e "Failed: ${RED}$FAILED${NC} dashboards"
    if [ -n "$FAILED_DASHBOARDS" ]; then
        echo -e "Failed dashboards:$FAILED_DASHBOARDS"
    fi
fi

echo ""
echo "Current dashboards in project:"
echo "----------------------------------------"
gcloud monitoring dashboards list \
    --project="$PROJECT_ID" \
    --format="table(displayName,name.segment(-1):label=DASHBOARD_ID)" 2>/dev/null || \
    echo "Could not list dashboards"

echo ""
echo "=========================================="
echo " Next Steps"
echo "=========================================="
echo "1. View dashboards at:"
echo "   https://console.cloud.google.com/monitoring/dashboards?project=$PROJECT_ID"
echo ""
echo "2. To manually update a specific dashboard:"
echo "   gcloud monitoring dashboards update <DASHBOARD_ID> \\"
echo "     --config-from-file=<dashboard_file.json> \\"
echo "     --project=$PROJECT_ID"
echo ""

if [ "$FAILED" -gt 0 ]; then
    echo "3. To troubleshoot failed imports, run with debug:"
    echo "   bash -x $0"
    echo ""
fi

# Exit with appropriate code
if [ "$FAILED" -gt 0 ]; then
    exit 1
else
    exit 0
fi