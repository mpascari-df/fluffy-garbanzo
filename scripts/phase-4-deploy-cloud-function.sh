# phase-4-deploy-cloud-function.sh
# This script deploys the Cloud Function that processes Pub/Sub messages
# and writes them to a GCS bucket.
# -----------------------------------------------------------------------------
# IMPORTANT: This script will exit on any error.
# -----------------------------------------------------------------------------
set -e

# --- Sourcing Configuration ---
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CONFIG_FILE="${SCRIPT_DIR}/config.sh"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "ERROR: Configuration file '$CONFIG_FILE' not found."
    exit 1
fi
source "$CONFIG_FILE"

# --- Configuration Validation ---
# We no longer check for SERVICE_ACCOUNT here, as we are using the default
if [ -z "$PROJECT_ID" ] || [ -z "$PUBSUB_TOPIC_NAME" ] || [ -z "$GCS_DATA_BUCKET_NAME" ] || [ -z "$CLOUD_FUNCTION_NAME" ]; then
    echo "ERROR: PROJECT_ID, PUBSUB_TOPIC_NAME, GCS_DATA_BUCKET_NAME, and CLOUD_FUNCTION_NAME must be set in '$CONFIG_FILE'."
    exit 1
fi

echo "--- Configuration for Phase 4 ---"
echo "Project ID:           $PROJECT_ID"
echo "Pub/Sub Topic:        $PUBSUB_TOPIC_NAME"
echo "GCS Data Bucket:      $GCS_DATA_BUCKET_NAME"
echo "Cloud Function Name:  $CLOUD_FUNCTION_NAME"
# Removed the Service Account line since we are no longer using a custom one.
echo "---------------------------------"

# Step 1: Enable necessary APIs for Cloud Functions
echo "Enabling required APIs for Cloud Functions..."
gcloud services enable \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  eventarc.googleapis.com \
  --project "$PROJECT_ID"

# Step 2: Deploy the Cloud Function
echo "Deploying Cloud Function '$CLOUD_FUNCTION_NAME'..."

CF_SOURCE_DIR="${SCRIPT_DIR}/../cloud-function-writer"
if [ ! -d "$CF_SOURCE_DIR" ]; then
    echo "ERROR: Cloud Function source directory '$CF_SOURCE_DIR' not found."
    echo "Please create this directory and place main.py and requirements.txt inside it."
    exit 1
fi

# Deploy the Cloud Function without the --service-account flag
# to use the default App Engine service account.
gcloud functions deploy "$CLOUD_FUNCTION_NAME" \
  --entry-point process_pubsub_message \
  --runtime python310 \
  --trigger-topic "$PUBSUB_TOPIC_NAME" \
  --set-env-vars "GCS_DATA_BUCKET_NAME=$GCS_DATA_BUCKET_NAME" \
  --region "europe-west1" \
  --project "$PROJECT_ID" \
  --source "$CF_SOURCE_DIR" \
  --memory 256MiB \
  --timeout 30s \

echo "Cloud Function '$CLOUD_FUNCTION_NAME' deployment initiated."
echo "You can check the deployment status and logs in the Google Cloud Console."
