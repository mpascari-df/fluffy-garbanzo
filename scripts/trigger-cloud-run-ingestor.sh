# trigger-cloud-run.sh
# This script sends an authenticated HTTP request to the Cloud Run service,
# effectively "waking it up" if it has scaled down to zero instances.
# -----------------------------------------------------------------------------
# IMPORTANT: This script will exit on any error.
# -----------------------------------------------------------------------------
set -e

# --- Sourcing Configuration ---
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CONFIG_FILE="${SCRIPT_DIR}/config_prod.sh"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "ERROR: Configuration file '$CONFIG_FILE' not found."
    exit 1
fi
source "$CONFIG_FILE"

# --- Configuration Validation ---
if [ -z "$PROJECT_ID" ] || [ -z "$CLOUD_RUN_SERVICE_NAME" ]; then
    echo "ERROR: PROJECT_ID and CLOUD_RUN_SERVICE_NAME must be set in '$CONFIG_FILE'."
    exit 1
fi

echo "--- Manually Triggering Cloud Run Service ---"
echo "Project ID:           $PROJECT_ID"
echo "Service Name:         $CLOUD_RUN_SERVICE_NAME"
echo "Region:               $REGION"
echo "-------------------------------------------"

# Step 1: Get an identity token for authentication
echo "Fetching authentication token..."
AUTH_TOKEN=$(gcloud auth print-identity-token)
if [ -z "$AUTH_TOKEN" ]; then
    echo "ERROR: Failed to fetch authentication token. Ensure you are logged in to gcloud."
    exit 1
fi
echo "Authentication token fetched."

# Step 2: Get the Cloud Run service URL
SERVICE_NAME="$CLOUD_RUN_SERVICE_NAME"
SERVICE_URL=$(gcloud run services describe "$SERVICE_NAME" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  --format='value(status.url)')

if [ -z "$SERVICE_URL" ]; then
    echo "ERROR: Failed to get Cloud Run service URL for '$SERVICE_NAME'."
    exit 1
fi
echo "Cloud Run Service URL: $SERVICE_URL"

# Step 3: Send an authenticated HTTP request to the service
echo "Sending authenticated request to Cloud Run service..."
# -s: silent mode (don't show progress meter or error messages)
# -w "\n%{http_code}": write HTTP status code to stdout after the response body, followed by a newline
curl_output=$(curl -s -w "\n%{http_code}" -H "Authorization: Bearer ${AUTH_TOKEN}" "${SERVICE_URL}")

HTTP_STATUS=$(echo "$curl_output" | tail -n1)
RESPONSE_BODY=$(echo "$curl_output" | head -n -1)

echo "Response Body:"
echo "$RESPONSE_BODY"
echo "HTTP Status Code: $HTTP_STATUS"

if [ "$HTTP_STATUS" -eq 200 ]; then
    echo "Successfully triggered Cloud Run service."
else
    echo "ERROR: Failed to trigger Cloud Run service. HTTP Status: $HTTP_STATUS"
    exit 1
fi

echo "Manual trigger script finished."
